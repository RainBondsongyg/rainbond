package dao

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/goodrain/rainbond/db/model"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

// capability_id: rainbond.cleanup.version-update-no-resurrection
func TestVersionUpdateNeverRecreatesRetiredRecordsOrResetsActivation(t *testing.T) {
	for _, scenario := range []struct {
		name                string
		affected, remaining int64
	}{{"changed", 1, 1}, {"unchanged", 0, 1}, {"retired", 0, 0}} {
		t.Run(scenario.name, func(t *testing.T) {
			raw, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherFunc(func(expected, actual string) error {
				if strings.HasPrefix(actual, "UPDATE") {
					if strings.Contains(actual, "activation_revision") {
						return fmt.Errorf("stale callback overwrites activation checkpoint")
					}
					if !strings.Contains(actual, "`cmd`") {
						return fmt.Errorf("zero-value command update was lost")
					}
				}
				if matched, err := regexp.MatchString(expected, actual); err != nil || !matched {
					return fmt.Errorf("unexpected SQL: %s", actual)
				}
				return nil
			})))
			if err != nil {
				t.Fatal(err)
			}
			database, err := gorm.Open("mysql", raw)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			database.LogMode(false)
			mock.ExpectBegin()
			mock.ExpectExec("UPDATE .*tenant_service_version.* WHERE .*ID.*service_id.*build_version.*event_id").WillReturnResult(sqlmock.NewResult(0, scenario.affected))
			mock.ExpectCommit()
			if scenario.affected == 0 {
				mock.ExpectQuery("SELECT count.*tenant_service_version").WithArgs(uint(7), "service", "old", "event").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(scenario.remaining))
			}
			record := &model.VersionInfo{Model: model.Model{ID: 7}, ServiceID: "service", BuildVersion: "old", EventID: "event", ActivationRevision: "stale", Cmd: ""}
			err = (&VersionInfoDaoImpl{DB: database}).UpdateModel(record)
			if scenario.remaining == 0 && err != gorm.ErrRecordNotFound {
				t.Fatalf("retired record not rejected: %v", err)
			}
			if scenario.remaining > 0 && err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
