package main

// Config represents config structure for segments cmd.
type Config struct {
	PythiaSegmentsAddr string `envconfig:"addr" required:"true"`
	Debug              bool   `envconfig:"debug" required:"false"`

	PostgresAddr   string `envconfig:"postgres_addr" required:"true"`
	PostgresUser   string `envconfig:"postgres_user" required:"true"`
	PostgresPasswd string `envconfig:"postgres_passwd" required:"true"`
	PostgresDBName string `envconfig:"postgres_dbname" required:"true"`
}
