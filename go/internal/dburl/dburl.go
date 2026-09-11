package dburl

import (
	"fmt"
	"os"
)

func URL() string {
	port := os.Getenv("POSTGRES_PORT")
	if port == "" {
		port = "5432"
	}

	return fmt.Sprintf("postgres://pgledger:pgledger@localhost:%s/pgledger", port)
}
