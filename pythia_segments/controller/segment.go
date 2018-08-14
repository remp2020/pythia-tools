package controller

import (
	"github.com/goadesign/goa"
	"gitlab.com/remp/pythia/cmd/pythia_segments/app"
	"gitlab.com/remp/pythia/cmd/pythia_segments/model"
)

// SegmentType represents type of segment (source of data used for segment)
type SegmentType int

// Enum of available segment types
const (
	UserSegment SegmentType = iota + 1
	BrowserSegment
)

// SegmentController implements the segment resource.
type SegmentController struct {
	*goa.Controller
	SegmentStorage model.SegmentStorage
}

// NewSegmentController creates a segment controller.
func NewSegmentController(service *goa.Service, segmentStorage model.SegmentStorage) *SegmentController {
	return &SegmentController{
		Controller:     service.NewController("SegmentController"),
		SegmentStorage: segmentStorage,
	}
}

// List runs the list action.
func (c *SegmentController) List(ctx *app.ListSegmentsContext) error {
	sc := c.SegmentStorage.List()
	return ctx.OK((SegmentCollection)(sc).ToMediaType())
}

// CheckUser runs the check_user action.
func (c *SegmentController) CheckUser(ctx *app.CheckUserSegmentsContext) error {
	s, ok := c.SegmentStorage.Get(ctx.SegmentCode)
	if !ok {
		return ctx.NotFound()
	}

	ok = c.SegmentStorage.CheckUser(s, ctx.UserID)
	return ctx.OK(&app.SegmentCheck{
		Check: ok,
	})
}

// CheckBrowser runs the check_browser action.
func (c *SegmentController) CheckBrowser(ctx *app.CheckBrowserSegmentsContext) error {
	s, ok := c.SegmentStorage.Get(ctx.SegmentCode)
	if !ok {
		return ctx.NotFound()
	}

	ok = c.SegmentStorage.CheckBrowser(s, ctx.BrowserID)
	return ctx.OK(&app.SegmentCheck{
		Check: ok,
	})
}

// Users runs the users action.
func (c *SegmentController) Users(ctx *app.UsersSegmentsContext) error {
	s, ok := c.SegmentStorage.Get(ctx.SegmentCode)
	if !ok {
		return ctx.NotFound()
	}

	uc := c.SegmentStorage.ListUsers(s)
	return ctx.OK(uc)
}
