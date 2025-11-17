# SQLite
This is a no CGO sqlite driver for GOE ORM based on https://pkg.go.dev/modernc.org/sqlite.

## Features

- 🪝 Connection Hook
- 🧪 In Memory Database

## Usage

### Basic

```go
package main

import (
	"github.com/go-goe/goe"
	"github.com/go-goe/sqlite"
)

type User struct {
	ID    int
	Name  string
	Email string `goe:"unique"`
}

type Database struct {
	User *User
	*goe.DB
}

func main() {
	db, err := goe.Open[Database](sqlite.Open("goe.db", sqlite.NewConfig(sqlite.Config{})))
}
```

### Connection Hook

```go
package main

import (
	"github.com/go-goe/goe"
	"github.com/go-goe/sqlite"
)

type User struct {
	ID    int
	Name  string
	Email string `goe:"unique"`
}

type Database struct {
	User *User
	*goe.DB
}

func main() {
	db, err := goe.Open[Database](sqlite.Open(filepath.Join(os.TempDir(), "goe.db"), sqlite.NewConfig(
		sqlite.Config{
			ConnectionHook: func(conn sqlite.ExecQuerierContext, dsn string) error {
				conn.ExecContext(context.Background(), "PRAGMA foreign_keys = OFF;", nil)
				return nil
			},
		},
	)))
}
```

### In Memory Database

```go
package main

import (
	"github.com/go-goe/goe"
	"github.com/go-goe/sqlite"
)

type User struct {
	ID    int
	Name  string
	Email string `goe:"unique"`
}

type Database struct {
	User *User
	*goe.DB
}

func main() {
	db, err := goe.Open[Database](sqlite.OpenInMemory(sqlite.NewConfig(sqlite.Config{})))
}
```