package design

import (
	. "github.com/goadesign/goa/design"
	. "github.com/goadesign/goa/design/apidsl"
)

var _ = API("pythia_segments", func() {
	Title("Pythia Segments")
	Description("Provides segments API for pythia-generated predictions")
	License(func() {
		Name("MIT")
	})
	Scheme("http")
	Consumes("application/json")
	Produces("application/json")
	Origin("*", func() {
		Methods("GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS")
		Headers("Content-Type", "Authorization")
	})
	ResponseTemplate(BadRequest, func() {
		Description("Invalid request sent")
		Status(400)
		Media(ErrorMedia)
	})
	Host("localhost:8083")
})
