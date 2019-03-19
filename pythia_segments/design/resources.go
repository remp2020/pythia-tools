package design

import (
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

// set of constants reused within multiple actions
const (
	SegmentPattern = `^[a-zA-Z0-9_\-@.]+$`
	UserPattern    = `^[a-zA-Z0-9_\-@.]+$`
)

var _ = Resource("swagger", func() {
	Origin("*", func() {
		Methods("GET", "OPTIONS")
		Headers("*")
	})
	NoSecurity()
	Files("/swagger.json", "swagger/swagger.json")
})

var _ = Resource("segments", func() {
	Description("Segment operations")
	BasePath("/segments")
	NoSecurity()

	Action("list", func() {
		Description("List all segments.")
		Routing(GET("/"))
		Response(NotFound)
		Response(BadRequest)
		Response(OK, func() {
			Media(CollectionOf(Segment, func() {
				View("default")
			}))
		})
	})
	Action("check_user", func() {
		Description("Check whether given user ID belongs to segment.")
		Routing(GET("/:segment_code/users/check/:user_id"))
		Params(func() {
			Param("segment_code", String, "Segment code", func() {
				Pattern(SegmentPattern)
			})
			Param("user_id", String, "User ID", func() {
				Pattern(UserPattern)
			})
		})
		Response(NotFound)
		Response(BadRequest)
		Response(OK, func() {
			Media(SegmentCheck)
		})
	})
	Action("check_browser", func() {
		Description("Check whether given browser ID belongs to segment.")
		Routing(GET("/:segment_code/browsers/check/:browser_id"))
		Params(func() {
			Param("segment_code", String, "Segment code", func() {
				Pattern(SegmentPattern)
			})
			Param("browser_id", String, "Browser ID", func() {
				Pattern(UserPattern)
			})
		})
		Response(NotFound)
		Response(BadRequest)
		Response(OK, func() {
			Media(SegmentCheck)
		})
	})
	Action("users", func() {
		Description("List users of segment.")
		Routing(
			GET("/:segment_code/users"),
		)
		Params(func() {
			Param("segment_code", String, "Segment code", func() {
				Pattern(SegmentPattern)
			})
		})
		Response(NotFound)
		Response(BadRequest)
		Response(OK, ArrayOf(String))
	})
})
