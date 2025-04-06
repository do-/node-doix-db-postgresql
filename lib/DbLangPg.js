const {DbLang, DbRelation, DbTable, DbView, DbProcedure, DbFunction, DbColumn, DbType, DbTypeArithmeticInt, DbTypeCharacter, DbTypeDate, DbTypeTimestamp, DbTypeArithmeticFixed} = require ('doix-db')

const CH_QUEST = '?'.charAt (0)
const CH_QUOTE = "'".charAt (0)
const CH_SLASH = '/'.charAt (0)
const CH_ASTER = '*'.charAt (0)
const CH_MINUS = '-'.charAt (0)

const CH_C_UC = 'C'.charCodeAt (0)
const CH_C_LC = 'c'.charCodeAt (0)

const TYPE_MAP = new Map ([ 
	['TEXT', DbTypeCharacter],
	['DATE', DbTypeDate],
	['TIMESTAMP', DbTypeTimestamp],
	['NUMERIC', DbTypeArithmeticFixed],
].map (([name, clazz]) => [name, new clazz ({name})]))

const COPY_OPTIONS = new Set ([
	'FORMAT',
	'FREEZE',
	'DELIMITER',
	'NULL',
	'DEFAULT',
	'HEADER',
	'QUOTE',
	'ESCAPE',
	'FORCE_QUOTE',
	'FORCE_NOT_NULL',
	'FORCE_NULL',
	'ENCODING'
])

{

	for (let bytes = 2; bytes <= 8; bytes <<= 1) {

		let name = 'INT' + bytes

		TYPE_MAP.set (name, new DbTypeArithmeticInt  ({name, bytes}))

	}

	for (const [name, aliases] of [
		['INT2',  ['SMALLINT']],
		['INT4',  ['INT', 'INTEGER']],
		['INT8',  ['BIGINT']],
		['NUMERIC',  ['DECIMAL']],
	]) for (const alias of aliases) TYPE_MAP.set (alias, TYPE_MAP.get (name))

}

class DbLangPg extends DbLang {

	static MAP_TYPE_ID_2_NAME = new Map (
		Object.entries (require ('pg-types').builtins)
			.map (kv => kv.reverse ())
	)

	constructor () {
	
		super ()
		
	}

	isCopyStatement (sql) {

		sql = sql.trimStart (); switch (sql.charCodeAt (0)) {

			case CH_C_UC:
				return sql.startsWith ('COPY') || sql.startsWith ('Copy')

			case CH_C_LC:
				return sql.startsWith ('copy')
		
			default:
				return false

		}
	
	}	

	getTypeDefinition (name) {

		name = name.toUpperCase ()

		if (TYPE_MAP.has (name)) return TYPE_MAP.get (name)

		return new DbType ({name})

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

			name = name.toUpperCase (); if (!COPY_OPTIONS.has (name)) continue

			if (sql.length !== 0) sql += ', '

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

		if (options.result === 'record') result [result.length - 1] += ` RETURNING *`

		return result

	}

	toParamsSql (query) {

		const result = super.toParamsSql (query), {options} = query

		if ('limit' in options) this.addLimitOffset (result, options.limit, options.offset)

		return result
	
	}

	normalizeSQL (call) {

		let src = call.sql

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
							if (last < 0) {
								call.sql = dst
								return
							}
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

		call.sql = dst + src.slice (last)

	}
	
	genSelectColumnsSql () {

		const schemata = [...this.model.schemata.values ()].map (i => this.quoteStringLiteral (i.schemaName))

		return /*sql*/`
			WITH p AS (
				SELECT 
					t.table_name 
					, t.table_schema
					, t.constraint_type
					, t.constraint_name
					, u.column_name 
					, u.ordinal_position 
				FROM 
					information_schema.table_constraints t
					join information_schema.key_column_usage u ON  
						u.table_catalog = current_catalog 
						and u.table_schema IN (${schemata})
						and u.table_name = t.table_name 
						and u.constraint_name = t.constraint_name 
				WHERE 
					t.table_catalog = current_catalog 
					and t.table_schema IN (${schemata})
					and t.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
			)
			, td AS (
				SELECT
					c.relname table_name
					, n.nspname table_schema
					, d.description AS comment
				FROM 
					pg_namespace n
					JOIN pg_class c ON c.relnamespace = n.oid
					JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
				WHERE
					n.nspname IN (${schemata})		
			)
			, pk AS (
				SELECT 
					p.table_name 
					, p.table_schema
					, JSON_AGG (p.column_name order by ordinal_position) AS pk
				FROM
					p
				WHERE
					constraint_type = 'PRIMARY KEY'
				GROUP BY
					1, 2
			)
			, fk AS (
				SELECT 
					p.table_name 
					, p.table_schema
					, JSON_AGG (p.constraint_name) AS _fk_names
				FROM
					p
				WHERE
					constraint_type = 'FOREIGN KEY'
				GROUP BY
					1, 2
			)
			, ix AS (
				SELECT 
					tablename AS table_name
					, schemaname AS table_schema
					, JSON_AGG (indexname) AS _ix_names
				FROM 
					pg_indexes
				WHERE 
					schemaname IN (${schemata})
				GROUP BY
					1, 2
			)
			, trg AS (
				SELECT
					t.event_object_table AS table_name
					, JSON_AGG (DISTINCT TRIM (LEFT (SUBSTRING (t.action_statement, 18), -2))) AS _trg_proc_names
				FROM
					information_schema.triggers t
				WHERE 
					t.trigger_catalog = current_catalog
					AND t.trigger_schema IN (${schemata})
				GROUP BY
					1
			)
			, t AS (
				SELECT
					t.table_name AS name
					, pk.pk
				FROM 
					information_schema.tables t
					join pk on t.table_name = pk.table_name
				WHERE 
					t.table_type = 'BASE TABLE'
					AND t.table_catalog = current_catalog
					AND t.table_schema  IN (${schemata})
			)
			, tcd AS (
				SELECT
					c.relname table_name            	
					, a.attname AS name
					, d.description AS "comment"
				FROM 
					pg_namespace ns
					JOIN pg_class       c ON c.relnamespace = ns.oid
					JOIN pg_attribute   a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
					JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
				WHERE
					ns.nspname IN (${schemata})
			)
			, c AS (
				SELECT
						c.table_name as "table"
						, table_schema
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
						, tcd.comment
				FROM
					information_schema.columns c
					LEFT JOIN tcd ON c.table_name = tcd.table_name AND c.column_name = tcd.name
				WHERE 
					c.table_catalog = current_catalog
					and c.table_schema IN (${schemata})
			)
			, tc AS (
				SELECT 
					c.table as name
					, table_schema
					, json_object_agg(c.name, c.*) AS "columns"
				FROM 
					c
				GROUP BY 
					1, 2
			)
			SELECT 
				tc.* 
				, td.comment
				, pk.pk
				, fk._fk_names
				, COALESCE (ix._ix_names, '[]'::JSON) AS _ix_names
				, trg._trg_proc_names
			FROM 
				tc
				INNER JOIN pk  ON tc.name =  pk.table_name AND tc.table_schema = pk.table_schema
				LEFT  JOIN td  ON tc.name =  td.table_name AND tc.table_schema = td.table_schema
				LEFT  JOIN fk  ON tc.name =  fk.table_name AND tc.table_schema = pk.table_schema
				LEFT  JOIN ix  ON tc.name =  ix.table_name AND tc.table_schema = ix.table_schema
				LEFT  JOIN trg ON tc.name = trg.table_name
		`

	}

	genReCreateAsMock (o) {
	
		if (o instanceof DbView) return this.genReCreateViewAsMock (o)

		if (o instanceof DbProcedure) return this.genReCreateProcedureAsMock (o)

		if (o instanceof DbFunction) return this.genReCreateFunctionAsMock (o)
		
		throw Error (`Don't know how to create a mock for ` + o.constructor.name)

	}

	genReCreateViewAsMock ({qName, columns}) {
	
		const nulls = Object.values (columns)
		
			.map (({qName, typeDim}) => `NULL::${typeDim} AS ${qName}`)

		return [
		
			`DROP VIEW IF EXISTS ${qName} CASCADE`,
	
			`CREATE OR REPLACE VIEW ${qName} AS SELECT ${nulls}`,
				
		]
	
	}
	
	genRoutineArgFull (p) {
	
		const s = `${p.mode} ${p.qName} ${p.type}`
		
		return p.default == null ? s : s + '=' + p.default
	
	}	

	genRoutineArgShort ({mode, type}) {
	
		return `${mode} ${type}`
	
	}	
		
	genReCreateProcedureAsMock ({qName, parameters}) {

		const args = parameters.map (i => this.genRoutineArgFull (i))
				
		return `CREATE OR REPLACE PROCEDURE ${qName} (${args}) LANGUAGE plpgsql AS 'BEGIN NULL; END'`

	}
	
	genReCreateFunctionAsMock ({qName, parameters, returns}) {

		const a    = parameters.map (i => this.genRoutineArgShort (i))

		const args = parameters.map (i => this.genRoutineArgFull (i))
		
		return [
		
			`DROP FUNCTION IF EXISTS ${qName} (${a}) CASCADE`,

			`CREATE FUNCTION ${qName} (${args}) RETURNS ${returns} LANGUAGE sql AS 'SELECT NULL::${returns}'`,
			
		]

	}

	genReCreate (o) {
	
		if (o instanceof DbView) return this.genReCreateView (o)

		if (o instanceof DbProcedure) return this.genReCreateProcedure (o)

		if (o instanceof DbFunction) return this.genReCreateFunction (o)
		
		throw Error (`Don't know how to recreate ` + o.constructor.name)

	}

	genReCreateView ({qName, options, specification, sql}) {

		return [

			`CREATE OR REPLACE ${options} VIEW ${qName} ${specification} AS ${sql}`,

		]

	}
	
	genReCreateProcedure ({qName, parameters, lang, body, options}) {
		
		const args = parameters.map (i => this.genRoutineArgFull  (i))
		
		return `CREATE OR REPLACE PROCEDURE ${qName} (${args}) LANGUAGE ${lang} ${options.join (' ')} AS $$${body}$$`

	}
	
	genReCreateFunction ({qName, parameters, returns, lang, body, options}) {
		
		const a    = parameters.map (i => this.genRoutineArgShort (i))

		const args = parameters.map (i => this.genRoutineArgFull  (i))
		
		return `CREATE OR REPLACE FUNCTION ${qName} (${args}) RETURNS ${returns} LANGUAGE ${lang} ${options.join (' ')} AS $$${body}$$`

	}

	genCreate (o) {
	
		if (o instanceof DbTable) return this.genCreateTable (o)
		
		throw Error (`Don't know how to create ` + o.constructor.name)

	}

	genComment (o) {

		const {qName, comment} = o

		let sql = 'COMMENT ON '

		sql += o instanceof DbView ? 'VIEW' : o.constructor.name.slice (2).toUpperCase ()

		sql += ' '

		if (o instanceof DbColumn) {

			sql += o.relation.qName

			sql += '.'
	
		}

		sql += qName

		sql += ' IS '

		sql += this.quoteLiteral (comment)

		return sql
		
	}

	* genAlter (asIs, toBe) {

		if (toBe instanceof DbTable) {
		
			for (const qp of this.genAlterTable (asIs, toBe)) yield qp
			
		}
		else {

			throw Error (`Don't know how to alter ` + toBe.constructor.name)

		}

	}

	genCreateTable ({qName, pk, columns}) {
	
		const things = Object.values (columns).map (c => this.genColumnDefinition (c))

		things.push ('PRIMARY KEY (' + pk.map (k => columns [k].qName) + ')')

		return `CREATE TABLE ${qName} (${things})`

	}

	isEqualColumnDefault (asIs, toBe) {
	
		if (super.isEqualColumnDefault (asIs, toBe)) return true
		
		const o = asIs.default, n = toBe.default; if ((o == null) !== (n == null)) return false

		return o == `${n}::${asIs.type.toLowerCase ()}`

	}
	
	* genAlterTable (asIs, {qName}) {
	
		const {toDo} = asIs, actions = [], fillIn = column => [`UPDATE ${qName} SET ${column.qName} = ? WHERE ${column.qName} IS NULL`, [column.default]]

		if (toDo.has ('alter-column')) for (const column of toDo.get ('alter-column')) {

			const {name, qName, typeDim, nullable} = column, def = column.default, alter = `ALTER ${qName} `, {diff} = asIs.columns [name], diffNullable = diff.includes ('nullable')

			const act = sql => actions.push (alter + sql)

			if (diff.includes ('typeDim')) act (`TYPE ${typeDim}`)

			if (diff.includes ('default')) act (def == null ? 'DROP DEFAULT' : 'SET DEFAULT ' + def)

			if (diff.includes ('nullable')) {
			
				if (nullable) {
				
					act ('DROP NOT NULL')

				}
				else {

					yield fillIn (column)

					act ('SET NOT NULL')

				}

			}

		}

		if (toDo.has ('add-column')) for (const column of toDo.get ('add-column'))
		
			actions.push ('ADD ' + this.genColumnDefinition (column))

		yield [`ALTER TABLE ${qName} ${actions}`]

	}

	getRequiredMutation (asIs, toBe) {

		const {toDo} = asIs

		if (toDo.has ('add-column') || toDo.has ('alter-column')) return 'alter'

		return null
	
	}
	
	genDropTableForeignKeys ({localName, _fk_names}) {

		return [`ALTER TABLE ${this.quoteName (localName)} ` + _fk_names

			.map (s => this.quoteName (s))

			.map (s => `DROP CONSTRAINT ${s} CASCADE`)
		
		]
	
	}
	
	genDropTableTriggers ({localName, _trg_proc_names}) {
	
		const names = _trg_proc_names.map (s => this.quoteName (s))
		
		return [`DROP FUNCTION IF EXISTS ${names} CASCADE`]

	}

	genCreateIndex ({relation, localName, parts, options}) {
	
		let s = 'CREATE'

		for (const o of options) if (o === 'UNIQUE') s += ' ' + o
		
		s += ' INDEX IF NOT EXISTS' 

		s += ' ' + this.quoteName (localName)

		s += ' ON' 
		
		s += ' ' + relation.qName

		s += '(' + parts + ')'

		for (const o of options) if (o.slice (0, 5) === 'WHERE') s += ' ' + o
		
		return s

	}

	* genDropTableColumns () {

		const {asIs} = this.migrationPlan; if (!asIs) return

		for (const {qName, toDo} of asIs.values ()) {
			
			const columnsToDrop = toDo?.get ('drop-column'); if (columnsToDrop && columnsToDrop.length !== 0)

				yield `ALTER TABLE ${qName} ` + columnsToDrop.map (name => `DROP COLUMN ${this.quoteName (name)} CASCADE`)

		}

	}

	* genDropIndexes () {

		const {asIs, toBe} = this.migrationPlan; if (!toBe) return

		for (const o of toBe.values ()) if (o instanceof DbRelation) {

			if (!asIs.has (o.name)) continue

			const {_ix_names} = asIs.get (o.name), {keys} = o, qNames = [] 
			
			for (const name in keys) if (keys [name] === null)

				for (const localName of [name, this.getIndexName (o, {name})]) 
					
					if (_ix_names.includes (localName))					

						qNames.push (this.quoteName (localName))

			if (qNames.length !== 0) yield `DROP INDEX IF EXISTS ${qNames} CASCADE`

		}

	}

	* genCreateIndexes () {

		const {asIs, toBe} = this.migrationPlan; if (!toBe) return

		for (const o of toBe.values ()) if (o instanceof DbRelation) {

			const {keys} = o, old = asIs.get (o.name), _ix_names = old ? old._ix_names : []

			for (const key of Object.values (keys))

				if (key !== null && !_ix_names.includes (key.localName))
			
					yield this.genCreateIndex (key)

		}

	}

	* genCreateTriggers () {
		
		const {toBe} = this.migrationPlan; if (!toBe) return
		
		const QUOT = '$_TTT_$'

		for (const o of toBe.values ()) if (o instanceof DbTable && 'triggers' in o) {
		
			const {name, qName, triggers, schema: {prefix}} = o, {length} = triggers, max = (length - 1).toString ().length
			
			for (let i = 0; i < length; i ++) {
			
				const {options, phase, action, sql} = triggers [i], glob = this.quoteName (name + '_trg_' + i.toString ().padStart (max, '0'));

				if (sql !== null) {
				
					yield `CREATE FUNCTION ${prefix}${glob} () RETURNS trigger AS ${QUOT}${sql}${QUOT} LANGUAGE plpgsql;`

					yield `CREATE ${options} TRIGGER ${glob} ${phase} ON ${qName} ${action} EXECUTE PROCEDURE ${prefix}${glob} ();`
				}

			}
		
		}

	}

	* genUpsertData () {

		const {toBe} = this.migrationPlan; if (!toBe) return

		for (const o of toBe.values ()) if (o instanceof DbTable && 'data' in o) {
		
			const {pk, columns, data, qName} = o, cols = Object.values (columns)

			const names = [], expr = []; for (const col of cols) {

				names.push (col.qName)

				expr.push ('default' in col ? `COALESCE(${col.qName},${col.default})` : col.qName)

			}

			let sql = `INSERT INTO ${qName} (${names}) SELECT ${expr} FROM JSON_POPULATE_RECORDSET (NULL::${qName}, $1) ON CONFLICT `

			if (pk.length === cols.length) {

				sql += 'DO NOTHING'

			}
			else {

				const key = [], upd = []

				for (const {name, qName} of cols) {

					if (pk.includes (name)) {

						key.push (qName)

					}
					else {

						upd.push (qName + '=EXCLUDED.' + qName)

					}

				}

				sql += `(${key}) DO UPDATE SET ${upd}`

			}

			yield [sql, [JSON.stringify (data)]]
		
		}

	}

	* genCreateForeignKeys () {
		
		const {toBe} = this.migrationPlan; if (!toBe) return

		for (const o of toBe.values ()) if (o instanceof DbTable) {
		
			const addFK = [], {qName, columns} = o; for (const name in columns) {
			
				const column = columns [name]; 
				
				if (!('reference' in column)) continue

				const {targetRelation, targetColumn, on} = column.reference; if (!(targetRelation instanceof DbTable)) continue

				let s = `ADD FOREIGN KEY (${column.qName}) REFERENCES ${targetRelation.qName} (${targetColumn.qName})`

				for (const action in on) s += ` ON ${action} ${on [action]}`

				addFK.push (s + ' NOT VALID')

			}

			if (addFK.length !== 0) yield `ALTER TABLE ${qName} ${addFK}`

		}

	}

	* genDDL () {

		const {migrationPlan} = this; if (!migrationPlan) throw Error ('genDDL called without a migration plan')

		const {asIs, toDo} = migrationPlan

		for (const {schemaName} of this.model.schemata.values ()) if (schemaName)
			
			yield ['CREATE SCHEMA IF NOT EXISTS ' + this.quoteName (schemaName)]
		
		if (asIs) for (const o of asIs.values ()) {
		
			if (o._fk_names) yield this.genDropTableForeignKeys (o)

			if (o._trg_proc_names) yield this.genDropTableTriggers (o)

		}

		for (const s of this.genDropTableColumns ()) yield [s]

		if (toDo.has ('recreate')) for (const o of toDo.get ('recreate')) {
		
			const r = this.genReCreateAsMock (o)
			
			if (Array.isArray (r)) {

				for (const s of r) yield [s]

			}
			else {

				yield [r]

			}
			
		}

		if (toDo.has ('create'))   for (const o of toDo.get ('create'))   yield [this.genCreate (o)]

		if (toDo.has ('alter'))    for (const o of toDo.get ('alter')) for (const qp of this.genAlter (asIs.get (o.name), o)) yield qp

		if (toDo.has ('recreate')) for (const o of toDo.get ('recreate')) {

			const r = this.genReCreate (o)

			if (Array.isArray (r)) {

				for (const s of r) yield [s]

			}
			else {

				yield [r]

			}

		}

		for (const s of this.genDropIndexes ()) yield [s]

		for (const s of this.genCreateIndexes ()) yield [s]

		for (const s of this.genCreateTriggers ()) yield [s]

		for (const s of this.genCreateForeignKeys ()) yield [s]

		for (const s of this.genUpsertData ()) yield s

		if (toDo.has ('comment')) for (const o of toDo.get ('comment')) yield [this.genComment (o)]

	}

}

module.exports = DbLangPg