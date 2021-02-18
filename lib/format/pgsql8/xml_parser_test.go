package pgsql8_test

import (
	"fmt"
	"testing"

	"github.com/dbsteward/dbsteward/lib/format/pgsql8"
	"github.com/stretchr/testify/assert"

	"github.com/dbsteward/dbsteward/lib/model"
)

func TestXmlParser_Process(t *testing.T) {
	// NOTE: this is just the third test case from v1 tests/pgsql8/Pgsql8XmlParserTest.php testProcess
	// the first two are subsets of functionality, so we won't get much out of them

	doc := &model.Definition{
		Schemas: []*model.Schema{
			&model.Schema{
				Name:  "test_schema",
				Owner: "ROLE_OWNER",
				Tables: []*model.Table{
					&model.Table{
						Name:       "test_table",
						PrimaryKey: model.DelimitedList{"primary_id"},
						Owner:      "ROLE_OWNER",
						Partitioning: &model.TablePartition{
							Type: model.TablePartitionTypeModulo,
							Options: []*model.TablePartitionOption{
								{Name: "number", Value: "8"},
								{Name: "column", Value: "partition_id"},
							},
						},
						Columns: []*model.Column{
							{Name: "primary_id", Type: "serial", Nullable: true},
							{Name: "partition_id", Type: "bigint", Nullable: false},
						},
						Indexes: []*model.Index{
							&model.Index{
								Name:  "primary_id_idx",
								Using: model.IndexTypeBtree,
								Dimensions: []*model.IndexDim{
									{Name: "primary_id", Value: "primary_id"},
								},
								Conditions: []*model.IndexCond{
									{SqlFormat: model.SqlFormatPgsql8, Condition: "primary_id IS NOT NULL"},
								},
							},
						},
					},
				},
			},
		},
	}

	expected := &model.Definition{
		Schemas: []*model.Schema{
			&model.Schema{
				Name:  "test_schema",
				Owner: "ROLE_OWNER",
				Tables: []*model.Table{
					&model.Table{
						Name:       "test_table",
						PrimaryKey: model.DelimitedList{"primary_id"},
						Owner:      "ROLE_OWNER",
						Partitioning: &model.TablePartition{
							Type: model.TablePartitionTypeModulo,
							Options: []*model.TablePartitionOption{
								{Name: "number", Value: "8"},
								{Name: "column", Value: "partition_id"},
							},
						},
						Columns: []*model.Column{
							{Name: "primary_id", Type: "serial", Nullable: true},
							{Name: "partition_id", Type: "bigint", Nullable: false},
						},
					},
				},
				Triggers: []*model.Trigger{
					&model.Trigger{
						Name:      "test_table_part_trg",
						SqlFormat: model.SqlFormatPgsql8,
						Events:    model.DelimitedList{"INSERT"},
						Timing:    model.TriggerTimingBefore,
						ForEach:   model.TriggerForEachRow,
						Table:     "test_table",
						Function:  "_p_test_schema_test_table.insert_trigger()",
					},
				},
			},
			&model.Schema{
				Name:   "_p_test_schema_test_table",
				Tables: make([]*model.Table, 8), // initialized below
				Functions: []*model.Function{
					&model.Function{
						Name:        "insert_trigger",
						Returns:     "TRIGGER",
						Owner:       "ROLE_OWNER",
						Description: "DBSteward auto-generated for table partition of test_schema.test_table",
						Definitions: []*model.FunctionDefinition{
							&model.FunctionDefinition{
								Language:  "plpgsql",
								SqlFormat: model.SqlFormatPgsql8,
								Text: `DECLARE
	mod_result INT;
BEGIN
	mod_result := NEW.partition_id % 8;
	IF (mod_result = 0) THEN
		INSERT INTO _p_test_schema_test_table.partition_0 VALUES (NEW.*);
	ELSEIF (mod_result = 1) THEN
		INSERT INTO _p_test_schema_test_table.partition_1 VALUES (NEW.*);
	ELSEIF (mod_result = 2) THEN
		INSERT INTO _p_test_schema_test_table.partition_2 VALUES (NEW.*);
	ELSEIF (mod_result = 3) THEN
		INSERT INTO _p_test_schema_test_table.partition_3 VALUES (NEW.*);
	ELSEIF (mod_result = 4) THEN
		INSERT INTO _p_test_schema_test_table.partition_4 VALUES (NEW.*);
	ELSEIF (mod_result = 5) THEN
		INSERT INTO _p_test_schema_test_table.partition_5 VALUES (NEW.*);
	ELSEIF (mod_result = 6) THEN
		INSERT INTO _p_test_schema_test_table.partition_6 VALUES (NEW.*);
	ELSEIF (mod_result = 7) THEN
		INSERT INTO _p_test_schema_test_table.partition_7 VALUES (NEW.*);
	END IF;
	RETURN NULL;
END;`,
							},
						},
						Grants: []*model.Grant{
							{Roles: model.DelimitedList{"ROLE_APPLICATION"}, Permissions: model.CommaDelimitedList{"EXECUTE"}},
						},
					},
				},
			},
		},
	}

	// there should be one child table for each parition, with a check constraint
	for i := 0; i < 8; i++ {
		indexCopy := *doc.Schemas[0].Tables[0].Indexes[0]
		child := &model.Table{
			Name:           fmt.Sprintf("partition_%d", i),
			Owner:          "ROLE_OWNER",
			PrimaryKey:     model.DelimitedList{"primary_id"},
			InheritsSchema: "test_schema",
			InheritsTable:  "test_table",
			Constraints: []*model.Constraint{
				&model.Constraint{
					Type:       model.ConstraintTypeCheck,
					Name:       fmt.Sprintf("test_table_p_%d_chk", i),
					Definition: fmt.Sprintf(`((partition_id %% 8) = %d)`, i),
				},
			},
			// indexes are copied from the parent and renamed
			Indexes: []*model.Index{
				&indexCopy,
			},
		}
		child.Indexes[0].Name = fmt.Sprintf("%s_p%d", child.Indexes[0].Name, i)
		child.Indexes[0].Dimensions[0].Name = "primary_id_1"

		expected.Schemas[1].Tables[i] = child
	}

	// note that Process mutates the document in place
	pgsql8.NewXmlParser().Process(doc)
	assert.Equal(t, expected, doc)
}
