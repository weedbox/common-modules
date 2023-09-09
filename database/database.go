package database

import (
	"gorm.io/gorm"
)

type DatabaseConnector interface {
	GetDB() *gorm.DB
}
