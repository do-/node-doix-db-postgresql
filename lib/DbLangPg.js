const {DbLang, DbView} = require ('doix-db')

const CH_QUEST = '?'.charAt (0)
const CH_QUOTE = "'".charAt (0)
const CH_SLASH = '/'.charAt (0)
const CH_ASTER = '*'.charAt (0)
const CH_MINUS = '-'.charAt (0)

const MAP_TYPE_ID_2_NAME = new Map ()
const MAP_TYPE_NAME_2_ID = new Map ()
		
for (const [name, id] of Object.entries (require ('pg-types').builtins)) {

	MAP_TYPE_ID_2_NAME.set (id, name)

	MAP_TYPE_NAME_2_ID.set (name, id)

}

const TYPE_ALIASES = new Map ([
	['DECIMAL', 'NUMERIC'],
	['INT',     'INT4'   ],
	['INTEGER', 'INT4'   ],
	['STRING',  'TEXT'   ],
])

class DbLangPg extends DbLang {

	constructor () {
	
		super ()
		
	}

	genComparisonRightPart (filter) {

		switch (filter.op) {

			case '~':
			case '~*':
			case '!~':
			case '!~*':
			case 'ILIKE':
			case 'NOT ILIKE':			
			case 'SIMILAR TO':
			case 'NOT SIMILAR TO':			
				return '?'

			default:
				return super.genComparisonRightPart (filter)

		}

	}

	getCanonicalTypeName (type) {
	
		let s = super.getCanonicalTypeName (type)
		
		if (MAP_TYPE_NAME_2_ID.has (s)) return s
		
		s = TYPE_ALIASES.get (s)
		
		if (MAP_TYPE_NAME_2_ID.has (s)) return s

		throw Error ('Unknown type: ' + type)
	
	}
	
	addLimitOffset (pq, limit, offset = 0) {

		const sql = pq.pop () + ' LIMIT ? OFFSET ?'

		pq.push (limit)
		pq.push (offset)
		pq.push (sql)		
	
	}

	genCopyFromSql (name, columns, options) {
	
		let sql = ''; for (let column of columns) {

			if (sql.length !== 0) sql += ','

			sql += this.quoteName (column)

		}
		
		return `COPY ${this.quoteName (name)} (${sql}) FROM STDIN` + this.genCopyFromSqlOptions (options)
	
	}

	genCopyFromSqlOptions (options) {
	
		const entries = Object.entries (options); if (entries.length === 0) return ''
	
		let sql = ''; for (let [name, value] of entries) {

			if (sql.length !== 0) sql += ', '

			name = name.toUpperCase ()

			sql += name + ' ' + this.genCopyFromSqlOptionValue (name, value)

		}
		
		return ' WITH (' + sql + ')'
	
	}

	genCopyFromSqlOptionValue (name, value) {

		if (name === 'FORMAT') return value
		
		if (name === 'FORCE_QUOTE' && value === '*') return value

		if (Array.isArray (value)) return '(' + value.map (s => this.quoteName (s)) + ')'
		
		return this.quoteLiteral (value)

	}

	genUpsertParamsSql (name, data, options) {

		const {key} = options

		let fields = ''; for (const k in data) {

			if (fields.length !== 0) fields += ','
			
			const q = this.quoteName (k)

			fields += `${q}=EXCLUDED.${q}`

		}

		const result = super.genInsertParamsSql (name, data)

		result [result.length - 1] += ` ON CONFLICT (${key}) DO UPDATE SET ${fields}`
		
		return result
	
	}

	genInsertParamsSql (name, data, options) {

		const result = super.genInsertParamsSql (name, data, options)

		if (options.onlyIfMissing === true) result [result.length - 1] += ` ON CONFLICT DO NOTHING`

		return result

	}

	toParamsSql (query) {

		const result = super.toParamsSql (query), {options} = query

		if ('limit' in options) this.addLimitOffset (result, options.limit, options.offset)

		return result
	
	}

	normalizeSQL (src) {

		const ST_SQL     = 0
		const ST_LITERAL = 1
		const ST_COMMENT = 2

		const {length} = src

		let n = 0, dst = '', last = 0, next = -1, depth = 0, state = ST_SQL

		while (next < length) {

			next ++; const c = src.charAt (next)

			switch (state) {

				case ST_LITERAL:
					if (c === CH_QUOTE && src.charAt (next + 1) !== CH_QUOTE) state = ST_LITERAL
					break

				case ST_SQL:

					switch (c) {

						case CH_QUEST:
							const s = src.slice (last, next)
							dst += s
							dst += s.trim ().slice (-7) === '::jsonb' ? '?' : '$' + (++ n)
							last = next + 1
							break

						case CH_QUOTE:
							state = ST_LITERAL
							break

						case CH_SLASH:
							if (src.charAt (next + 1) !== CH_ASTER) break
							dst += src.slice (last, next)
							state = ST_COMMENT
							depth = 1
							next ++
							break

						case CH_MINUS:
							if (src.charAt (next + 1) !== CH_MINUS) break
							dst += src.slice (last, next)
							last = src.indexOf ('\n', next)
							if (last < 0) return dst
							next = last
							break

					}
					break

				case ST_COMMENT:

					if (c !== CH_SLASH) break				

					if (src.charAt (next - 1) === CH_ASTER) {
						depth --
						if (depth > 0) break
						state = ST_SQL
						last = next + 1
					}
					else if (src.charAt (next + 1) === CH_ASTER) {
						depth ++
					}
					break

			}

		}

		return dst + src.slice (last)

	}
	
	genSelectColumnsSql () {

		return `
			WITH p AS (
				SELECT 
					t.table_name 
					, u.column_name 
					, u.ordinal_position 
				FROM 
					information_schema.table_constraints t
					join information_schema.key_column_usage u ON  
						u.table_catalog = current_catalog 
						and u.table_schema = current_schema
						and u.table_name = t.table_name 
						and u.constraint_name = t.constraint_name 
				WHERE 
					t.table_catalog = current_catalog 
					and t.table_schema = current_schema
					and t.constraint_type = 'PRIMARY KEY'
			)
			, pk AS (
				SELECT 
					p.table_name 
					, json_agg (p.column_name order by ordinal_position) AS pk
				FROM
					p
				GROUP BY
					1
			)
			, t AS (
				SELECT
					t.table_name AS name
					, obj_description ((t.table_schema||'.'||t.table_name)::regclass::oid) AS "comment"
					, pk.pk
				FROM 
					information_schema.tables t
					join pk on t.table_name = pk.table_name
				WHERE 
					t.table_type = 'BASE TABLE'
					AND t.table_catalog = current_catalog
					AND t.table_schema = current_schema ()
			)
			, c AS (
				SELECT
						c.table_name as "table"
						, c.column_name AS name
						, c.udt_name AS type
						, c.column_default AS "default"
						, c.is_nullable = 'YES' nullable
						, CASE
							WHEN c.character_maximum_length IS NOT NULL THEN c.character_maximum_length
							WHEN c.numeric_precision_radix = 10 THEN c.numeric_precision
							ELSE NULL
						END size
						, CASE
							WHEN c.numeric_precision_radix = 10 THEN c.numeric_scale
							ELSE NULL
						END scale
						, col_description((c.table_schema||'.'||c.table_name)::regclass::oid, ordinal_position) "comment"
				FROM
					information_schema.columns c
				WHERE 
					c.table_catalog = current_catalog
					and c.table_schema = current_schema
			)
			, tc AS (
				SELECT 
					c.table as name
					, json_object_agg(c.name, c.*) columns
				FROM 
					c
				GROUP BY 
					1
			)
			SELECT 
				tc.* 
				, pk.pk
			FROM 
				tc
				JOIN pk on tc.name = pk.table_name
		`

	}

	* genDropViews (model) {

		const qNames = new Set ()

		for (const v of model.allInstancesOf (DbView))
		
			qNames.add (v.qName)

		if (qNames.size !== 0)
			
			yield [`DROP VIEW IF EXISTS ${[...qNames.values ()]} CASCADE`]

	}

	* genCreateMockViews (model) {

		for (const v of model.allInstancesOf (DbView))

			yield [this.genCreateMockView (v)]

	}
	
	* genCreateViews (model) {

		for (const {qName, sql} of model.allInstancesOf (DbView))

			yield [`CREATE OR REPLACE VIEW ${qName} AS ${sql}`]

	}

	* genDDL (model) {
	
		for (const i of this.genDropViews (model)) yield i

		for (const i of this.genCreateMockViews (model)) yield i

		for (const i of this.genCreateViews (model)) yield i
	
	}
	
}

DbLangPg.MAP_TYPE_ID_2_NAME = MAP_TYPE_ID_2_NAME
DbLangPg.MAP_TYPE_NAME_2_ID = MAP_TYPE_NAME_2_ID

module.exports = DbLangPg