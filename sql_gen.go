package ...

import (
	"context"
	"fmt"
	"strings"

	".../internal/api"
	hlp ".../internal/helpers"
	lgr ".../pkg/logger"

	st ".../statuses"
)

const t_name = `table_name`
const t_alias = `p`

// функция генерации строки SELECT sql запроса
func GenQuerySelect(req *api.RequestApplicationsPools) string {
	lgr.LOG.Info(`-->> `, p_name+`.GenQuerySelect()`)

	// Добавление условий в строку запроса
	condition := addCondition(req)

	// Добавление строки пагинации в строку запроса
	pagination := hlp.AddPaginationToQuery(req.Limit, req.Page)

	query := `with ` + t_alias + ` as(select * from ` + t_name + ` ` + t_alias + ` ` + condition + pagination + `) select ` + strings.Join(fields_string, ", ") + ` from ` + t_alias + ` ` + join_string

	lgr.LOG.Info(`_QUERY_: `, hlp.DeleteTabsAndNewLinesSymbols(query))
	lgr.LOG.Info(`<<-- `, p_name+`.GenQuerySelect()`)
	return query
}

// функция генерации строки INSERT sql запроса
func GenQueryInsert(req *api.RequestApplicationsPools) string {
	lgr.LOG.Info(`-->> `, p_name+`.GenQueryInsert()`)

	insertFields := []string{}
	insertValues := []string{}

	insertFields = append(insertFields,
		`working_year`, `check_result_code`, `status_code`, `applications_source_id`,
		`is_sms_sent`, `payment_type_id`, `organisation_id`, `comment`,
		`created_by`, `updated_by`, `external_id`, `pool_count`,
		`fact_count`,
	)

	insertValues = append(insertValues,
		fmt.Sprintf(`%d`, req.WorkingYear), fmt.Sprintf(`%d`, req.CheckResultCode), fmt.Sprintf(`%d`, req.StatusCode), fmt.Sprintf(`%d`, req.ApplicationsSourceId),
		fmt.Sprintf(`%t`, req.IsSmsSent == api.Bool_TRUE), fmt.Sprintf(`%d`, req.PaymentTypeId), fmt.Sprintf(`%d`, req.OrganisationId), fmt.Sprintf(`'%s'`, req.Comment),
		fmt.Sprintf(`%d`, req.UserId), fmt.Sprintf(`%d`, req.UserId), fmt.Sprintf(`%d`, req.ExternalId), fmt.Sprintf(`%d`, req.PoolCount),
		fmt.Sprintf(`%d`, req.FactCount),
	)

	query := `with inserted as(insert into ` + t_name + ` (` + strings.Join(insertFields, `, `) + `) values(` + strings.Join(insertValues, `, `) + `) returning *) `
	query += `select ` + strings.Join(fields_string, ", ") + ` from inserted as ` + t_alias + ` ` + join_string

	lgr.LOG.Info(`_QUERY_: `, hlp.DeleteTabsAndNewLinesSymbols(query))
	lgr.LOG.Info(`<<-- `, p_name+`.GenQueryInsert()`)
	return query
}

// функция генерации строки UPDATE sql запроса
func GenQueryUpdate(req *api.RequestApplicationsPools) string {
	lgr.LOG.Info(`-->> `, p_name+`.GenQueryUpdate()`)
	query := `with updated as (UPDATE ` + t_name + ` SET `

	if req.WorkingYear != 0 {
		query += fmt.Sprintf(`working_year = %d, `, req.WorkingYear)
	}

	if req.CheckResultCode != 0 {
		query += fmt.Sprintf(`final_application_id = %d, `, req.CheckResultCode)
	}

	if req.StatusCode != 0 {
		query += fmt.Sprintf(`status_code = %d, `, req.StatusCode)
	}

	if req.ExternalId != 0 {
		query += fmt.Sprintf(`external_id = %d, `, req.ExternalId)
	}

  ...

	query += fmt.Sprintf(`updated_at = now(), updated_by = %d `, req.UserId)
	query += fmt.Sprintf(`where `+t_name+`.id = %d `, req.Id)
	query += `returning *) select ` + strings.Join(fields_string, ", ")
	query += ` from updated as ` + t_alias + ` ` + join_string

	lgr.LOG.Info(`_QUERY_: `, hlp.DeleteTabsAndNewLinesSymbols(query))
	lgr.LOG.Info(`<<-- `, p_name+`.GenQueryUpdate()`)
	return query
}

func GenQueryDelete(req *api.DeleteRequest) string {
	lgr.LOG.Info(`-->> `, p_name+`.GenQueryDelete()`)

	isDeleted := req.IsDeleted == api.Bool_TRUE

	query := `with deleted as (update ` + t_name + ` set is_deleted = %t, updated_at = now(), updated_by = %d `
	query = fmt.Sprintf(query, isDeleted, req.AuthorId)

	for ind, elem := range req.Ids {
		if ind == 0 {
			query += `WHERE ` + t_name + `.id = %d `
		} else {
			query += `OR ` + t_name + `.id = %d `
		}
		query = fmt.Sprintf(query, elem)
	}

	query += `RETURNING *) select ` + strings.Join(fields_string, ", ")
	query += `from deleted as ` + t_alias + ` ` + join_string

	lgr.LOG.Info(`<<-- `, p_name+`.GenQueryDelete()`)
	lgr.LOG.Info(`_QUERY_: `, hlp.DeleteTabsAndNewLinesSymbols(query))
	return query
}

// функция генерации строки SELECT COUNT sql запроса. Вернёт полное кол-во записей
func GenQueryCount(req *api.RequestApplicationsPools) string {
	lgr.LOG.Info(`-->> `, p_name+`.GenQueryCount()`)
	//строка запроса без условий
	query := `select count(*) from ` + t_name + ` as ` + t_alias + ` `

	//добавление условий в строку запроса
	query += addCondition(req)

	lgr.LOG.Info(`_QUERY_: `, hlp.DeleteTabsAndNewLinesSymbols(query))
	lgr.LOG.Info(`<<-- `, p_name+`.GenQueryCount()`)
	return query
}

// возвращаемые поля
var fields_string = []string{
	t_alias + `.id as id`,
	t_alias + `.working_year as working_year`,
	t_alias + `.pool_count as pool_count`,
	t_alias + `.check_result_code as check_result_code`,
	t_alias + `.status_code as status_code`,
	t_alias + `.applications_source_id as applications_source_id`,
	t_alias + `.is_sms_sent as is_sms_sent`,
	`to_char(` + t_alias + `.approoved_at, 'YYYY-MM-DD HH24:MI:SS'::text) as approoved_at`,
	t_alias + `.approoved_by as approoved_by`,
	`usr2.fullname as approoved_by_name`,
	t_alias + `.payment_type_id as payment_type_id`,
	t_alias + `.organisation_id as organisation_id`,
	t_alias + `.comment as comment`,
	t_alias + `.is_deleted as is_deleted`,
	`to_char(` + t_alias + `.created_at, 'YYYY-MM-DD HH24:MI:SS'::text) as created_at`,
	t_alias + `.created_by as created_by`,
	`usr.fullname as created_by_name`,
	`to_char(` + t_alias + `.updated_at, 'YYYY-MM-DD HH24:MI:SS'::text) as updated_at`,
	t_alias + `.updated_by as updated_by`,
	`usr1.fullname as updated_by_name`,
	t_alias + `.external_id as external_id`,
	`(select name from organizations_ref org where org.id = ` + t_alias + `.organisation_id) as organisation_name`,
	t_alias + `.fact_count as fact_count`,
}

const join_string = `
left join ... as usr on ` + t_alias + `.created_by = usr.id 
left join ... as usr1 on ` + t_alias + `.updated_by = usr1.id 
left join ... as usr2 on ` + t_alias + `.approoved_by = usr2.id 
`

func addCondition(req *api.RequestApplicationsPools) string {
	queryParts := []string{}

	if req.Id != 0 {
		queryParts = append(queryParts, fmt.Sprintf(t_alias+`.id = %d`, req.Id))
	}

	if req.WorkingYear != 0 {
		queryParts = append(queryParts, fmt.Sprintf(t_alias+`.working_year = %d`, req.WorkingYear))
	}

	if req.IsSmsSent != api.Bool_ANY {
		queryParts = append(queryParts, fmt.Sprintf(t_alias+`.is_sms_sent = %t`, req.IsSmsSent == api.Bool_TRUE))
	}

	...

	queryParts = append(queryParts, fmt.Sprintf(t_alias+`.is_deleted = %t`, req.IsDeleted == api.Bool_TRUE))

	if len(queryParts) > 0 {
		return `WHERE ` + strings.Join(queryParts, ` AND `)
	}

	return ``
}

func CheckFieldsInsert(ctx context.Context, req *api.RequestApplicationsPools) st.ResponseStatus {
	lgr.LOG.Info(`-->> `, p_name+`.CheckFieldsInsert()`)

	lgr.LOG.Info(`_ACTION_: `, `checking WorkingYear`)
	if req.WorkingYear == 0 {
		lgr.LOG.Warn(`_WARN_: `, st.GetStatus(401, ` WorkingYear`, ` must contain a value`))
		return st.GetStatus(401, ` WorkingYear`, ` must contain a value`)
	}

	lgr.LOG.Info(`_ACTION_: `, `checking ApplicationsSourceId`)
	if req.ApplicationsSourceId == 0 {
		lgr.LOG.Warn(`_WARN_: `, st.GetStatus(401, ` ApplicationsSourceId`, ` must contain a value`))
		return st.GetStatus(401, ` ApplicationsSourceId`, ` must contain a value`)
	}

	...

	lgr.LOG.Info(`<<-- `, p_name+`.CheckFieldsInsert()`)
	return st.GetStatus(100)
}

func CheckFieldsUpdate(ctx context.Context, req *api.RequestApplicationsPools) st.ResponseStatus {
	lgr.LOG.Info(`-->> `, p_name+`.CheckFieldsUpdate()`)

	lgr.LOG.Info(`_ACTION_: `, `checking Id`)
	if req.Id == 0 {
		lgr.LOG.Warn(`_WARN_: `, st.GetStatus(401, ` Id`, ` must contain a value`))
		return st.GetStatus(401, ` Id`, ` must contain a value`)
	}

	lgr.LOG.Info(`<<-- `, p_name+`.CheckFieldsUpdate()`)
	return st.GetStatus(100)
}

func CheckFieldsDelete(ctx context.Context, req *api.DeleteRequest) st.ResponseStatus {
	lgr.LOG.Info(`-->> `, p_name+`.CheckFieldsDelete()`)

	lgr.LOG.Info(`_ACTION_: `, `checking Ids and ApplicationId`)
	if len(req.Ids) == 0 && req.ApplicationId == 0 {
		lgr.LOG.Warn(`_WARN_: `, st.GetStatus(401, ` Ids or ApplicationId`, ` must contain a value`))
		return st.GetStatus(401, ` Ids or ApplicationId`, ` must contain a value`)
	}

	lgr.LOG.Info(`<<-- `, p_name+`.CheckFieldsDelete()`)
	return st.GetStatus(100)
}
