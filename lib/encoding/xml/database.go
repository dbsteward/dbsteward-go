package xml

import "github.com/dbsteward/dbsteward/lib/model"

type Database struct {
	SqlFormat    string          `xml:"sqlFormat"`
	Roles        *RoleAssignment `xml:"role"`
	ConfigParams []*ConfigParam  `xml:"configurationParameter"`

	// slony
}

type RoleAssignment struct {
	Application string        `xml:"application"`
	Owner       string        `xml:"owner"`
	Replication string        `xml:"replication"`
	ReadOnly    string        `xml:"readonly"`
	CustomRoles DelimitedList `xml:"customRole,omitempty"`
}

type ConfigParam struct {
	Name  string `xml:"name,attr"`
	Value string `xml:"value,attr"`
}

func (self *Database) ToModel() (*model.Database, error) {
	return &model.Database{
		SqlFormat: model.SqlFormat(self.SqlFormat),
		Roles: &model.RoleAssignment{
			Application: self.Roles.Application,
			Owner:       self.Roles.Owner,
			Replication: self.Roles.Replication,
			ReadOnly:    self.Roles.ReadOnly,
			CustomRoles: self.Roles.CustomRoles,
		},
		ConfigParams: []*model.ConfigParam{},
	}, nil
}
