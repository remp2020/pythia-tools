package design

import (
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var Segment = MediaType("application/vnd.segment+json", func() {
	Description("Segment check")
	Attributes(func() {
		Attribute("code", String, "Code-friendly identificator of segment")
		Attribute("name", String, "User-friendly name of segment")
		Attribute("group", SegmentGroup)
	})
	View("default", func() {
		Attribute("code")
		Attribute("name")
		Attribute("group")
	})
	Required("code", "name", "group")
})

var SegmentCheck = MediaType("application/vnd.segment.check+json", func() {
	Description("Segment check")
	Attributes(func() {
		Attribute("check", Boolean, "Flag whether user is in the segment or not")
	})
	View("default", func() {
		Attribute("check")
	})
	Required("check")
})

var SegmentGroup = MediaType("application/vnd.segment.group+json", func() {
	Description("Segment group")
	Attributes(func() {
		Attribute("name", String, "User-friendly name of segment group")
		Attribute("code", String, "URL friendly non-changing reference to segment")
		Attribute("sorting", Integer, "Sort order index")
	})
	View("default", func() {
		Attribute("name")
		Attribute("code")
		Attribute("sorting")
	})
	Required("name", "code", "sorting")
})
