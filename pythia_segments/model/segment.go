package model

import (
	"database/sql"
	"log"
	"reflect"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// possible prediction outcomes
const (
	OutcomeConversion    = "conversion"
	OutcomeNoConversion  = "no_conversion"
	OutcomeSharedAccount = "shared_account_login"
)

// SegmentStorage represents interface to get segment related data.
type SegmentStorage interface {
	Get(code string) (*Segment, bool)
	// List returns all available segments configured via Beam admin.
	List() SegmentCollection
	// CheckUser verifies presence of user within provided segment.
	CheckUser(segment *Segment, userID string) bool
	// CheckBrowser verifies presence of browser within provided segment.
	CheckBrowser(segment *Segment, browserID string) bool
	// Users return list of all users within segment.
	ListUsers(segment *Segment) []string
}

// Segment structure.
type Segment struct {
	ID     int
	Code   string
	Name   string
	Active bool

	Group *SegmentGroup `db:"segment_group"`
}

// SegmentCollection is list of Segments.
type SegmentCollection []*Segment

// SegmentGroup represents metadata about group, in which Segments can be placed in.
type SegmentGroup struct {
	ID      int
	Name    string
	Code    string
	Sorting int
}

// ConversionPrediction represents calculated result of users actions
type ConversionPrediction struct {
	Outcome   string `db:"predicted_outcome"`
	UserID    string `db:"user_id"`
	BrowserID string `db:"browser_id"`
}

// UserSet is set of Users keyed by userID.
type UserSet map[string]bool

// BrowserSet is set of Browser keyed by browserID.
type BrowserSet map[string]bool

// SegmentDB represents Segment's storage MySQL/InfluxDB implementation.
type SegmentDB struct {
	Postgres *sqlx.DB
	Segments map[string]*Segment

	Users    map[string]UserSet
	Browsers map[string]BrowserSet
}

// Get returns instance of Segment based on the given code.
func (sDB *SegmentDB) Get(code string) (*Segment, bool) {
	if sDB.Segments == nil {
		sDB.CacheSegments()
	}

	segment, ok := sDB.Segments[code]
	if !ok {
		return nil, false
	}

	return segment, true
}

// List returns all available segments configured via Beam admin.
func (sDB *SegmentDB) List() SegmentCollection {
	var sc SegmentCollection
	if sDB.Segments == nil {
		sDB.CacheSegments()
	}

	for _, s := range sDB.Segments {
		sc = append(sc, s)
	}
	return sc
}

// CheckUser verifies presence of user within provided segment.
func (sDB *SegmentDB) CheckUser(segment *Segment, userID string) bool {
	users, ok := sDB.Users[segment.Code]
	if !ok {
		return false
	}
	_, ok = users[userID]
	return ok
}

// CheckBrowser verifies presence of browser within provided segment.
func (sDB *SegmentDB) CheckBrowser(segment *Segment, browserID string) bool {
	browsers, ok := sDB.Browsers[segment.Code]
	if !ok {
		return false
	}
	_, ok = browsers[browserID]
	return ok
}

// ListUsers return list of all users within segment.
//
// Implementation is very simplified as a user can be present in multiple segments
// with different browsers. Having user in a "conversion" and "no_conversion"
// segment seems illogical and should be handled within this function.
func (sDB *SegmentDB) ListUsers(segment *Segment) []string {
	users, ok := sDB.Users[segment.Code]
	if !ok {
		return []string{}
	}
	var userIDs []string
	for id := range users {
		userIDs = append(userIDs, id)
	}
	return userIDs
}

// CacheSegments stores the segments in memory.
func (sDB *SegmentDB) CacheSegments() error {
	sm := make(map[string]*Segment)

	sg := &SegmentGroup{
		ID:      1,
		Name:    "Conversion probability (based on machine learned model)",
		Code:    "conversion_probability",
		Sorting: 100,
	}
	sc := SegmentCollection{
		&Segment{
			ID:     1,
			Name:   "Probably will convert",
			Code:   "conversion",
			Active: true,
			Group:  sg,
		},
		&Segment{
			ID:     2,
			Name:   "Probably will not convert",
			Code:   "no_conversion",
			Active: true,
			Group:  sg,
		},
		&Segment{
			ID:     2,
			Name:   "Sharing account (logged to account with active subscription recently)",
			Code:   "shared_account_login",
			Active: true,
			Group:  sg,
		},
	}

	old := sDB.Segments
	for _, s := range sc {
		sm[s.Code] = s
	}
	sDB.Segments = sm

	if !reflect.DeepEqual(old, sm) {
		log.Println("segment cache reloaded")
	}
	return nil
}

// CacheBrowsers refreshes browser cache of all segments. Data are selected for last
// processed date before provided "now".
func (sDB *SegmentDB) CacheBrowsers(now time.Time) error {
	cpc := []ConversionPrediction{}
	err := sDB.Postgres.Select(&cpc, `
SELECT browser_id, predicted_outcome
FROM conversion_predictions_daily
WHERE date = (
	SELECT MAX(date)
	FROM prediction_job_log
	WHERE date <= $1
)`, now.Format("2006-01-02"))

	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return errors.Wrap(err, "unable to cache browser segments from PostgreSQL")
	}

	browsers := make(map[string]BrowserSet)

	for _, pc := range cpc {
		if _, ok := browsers[pc.Outcome]; !ok {
			browsers[pc.Outcome] = make(BrowserSet)
		}
		browsers[pc.Outcome][pc.BrowserID] = true
	}
	sDB.Browsers = browsers
	return nil
}

// CacheUsers refreshes user cache of all segments. Data are selected for last
// processed date before provided "now".
func (sDB *SegmentDB) CacheUsers(now time.Time) error {
	cpc := []ConversionPrediction{}
	err := sDB.Postgres.Select(&cpc, `
SELECT user_id, predicted_outcome
FROM conversion_predictions_daily
WHERE date = (
	SELECT MAX(date)
	FROM prediction_job_log
	WHERE date <= $1
)
AND user_id IS NOT NULL
`, now.Format("2006-01-02"))

	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return errors.Wrap(err, "unable to cache user segments from PostgreSQL")
	}

	users := make(map[string]UserSet)

	for _, pc := range cpc {
		if _, ok := users[pc.Outcome]; !ok {
			users[pc.Outcome] = make(UserSet)
		}

		switch pc.Outcome {
		case OutcomeConversion:
			users[OutcomeConversion][pc.UserID] = true
			delete(users[OutcomeNoConversion], pc.UserID)
			delete(users[OutcomeSharedAccount], pc.UserID)
		case OutcomeSharedAccount:
			if _, ok := users[OutcomeConversion][pc.UserID]; ok {
				break
			}
			users[OutcomeSharedAccount][pc.UserID] = true
			delete(users[OutcomeNoConversion], pc.UserID)
		case OutcomeNoConversion:
			if _, ok := users[OutcomeConversion][pc.UserID]; ok {
				break
			}
			if _, ok := users[OutcomeSharedAccount][pc.UserID]; ok {
				break
			}
			users[OutcomeNoConversion][pc.UserID] = true
		}
	}
	sDB.Users = users
	return nil
}
