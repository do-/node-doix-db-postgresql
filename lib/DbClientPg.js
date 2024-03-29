const PgQueryStream = require ('pg-query-stream')
const PgCopyStream  = require ('pg-copy-streams')
const PgCursor      = require ('pg-cursor')
const {MAP_TYPE_ID_2_NAME} = require ('./DbLangPg.js')
const {DbClient, DbRoutine, DbFunction} = require ('doix-db')
const DbTableXformerPg = require ('./DbTableXformerPg.js')
const UUID = Symbol ('UUID')
const CALL = Symbol.for ('doixDbCall')

class DbClientPg extends DbClient {

	constructor (raw) {
	
		super ()

		this.raw = raw

		if (UUID in raw) this.uuid = raw [UUID]; else raw [UUID] = this.uuid
	
	}

	async begin () {
	
		await this.do ('BEGIN')

		this.txn = {}
	
	}

	async commit () {
	
		await this.do ('COMMIT')

		this.txn = null
	
	}

	async rollback () {
	
		await this.do ('ROLLBACK')

		this.txn = null
	
	}
	
	async release () {

		if (this.txn != null) await this.rollback ()
	
		this.raw.release ()
	
	}

	async exec (call) {

		{

			const {stack} = new Error ()

			call.prependListener ('error', err => {

				const {code, schema, table, constraint} = err; if (code == '23505' && constraint && table && schema) {

					for (const {schemaName, map} of this.model.schemata.values ()) if (schemaName === schema && map.has (table)) {

						const {keys} = map.get (table); for (const k of Object.values (keys)) if (k !== null && k.localName === constraint && 'message' in k)  {

							err.message = k.message (err)

						}

					}

				}

				const {message} = err, o = {message}; 
				
				for (const k in err) if (err [k] !== undefined) o [k] = err [k]
				
				err.stack = 'Error :' + JSON.stringify (o) + '\n' + stack.slice (1 + stack.indexOf ('\n'))

			})

		}

		{

			const onNotice = m => call.emit ('notice', m)
			
			const {raw} = this

			raw.on ('notice', onNotice)

			call.on ('finish', () => raw.off ('notice', onNotice))

		}

		const {raw} = this, {sql, params, options} = call, {checkOverflow, maxRows} = options, o = call.objectMode ? {} : {rowMode: 'array'}

		switch (maxRows) {

			case 0 : return call.raw = await raw.query (
				
				options.isPut ? PgCopyStream.from (sql)
				
				: {text: sql, values: params}
				
			)

			case Infinity : return call.rows = await raw.query (new PgQueryStream (sql, params, o))

			default       :

				const cursor = call.raw = await raw.query (new PgCursor (sql, params, o))

				return new Promise ((ok, fail) => {
	
					cursor.read (checkOverflow ? maxRows + 1: maxRows, (error, rows) => {
	
						try {

							if (error) throw error

							if (checkOverflow && rows.length > maxRows) throw Error ('Result set overflow, maxRows = ' + maxRows)

							call.columns = cursor._result.fields.map (i => {i.type = MAP_TYPE_ID_2_NAME.get (i.dataTypeID); return i})
		
							call.rows = rows

							ok ()

						}
						catch (x) {

							fail (x)

						}
						finally {

							cursor.close ()

						}
						
					})
			
				}

			)

		}
		
	}
	
	async invoke (name, params = []) {

		const routine = this.model.find (name); if (!routine) throw Error ('Routine not found: ' + name)

		if (!(routine instanceof DbRoutine)) throw Error ('Not a routine to be called: ' + name)

		const {length} = params, args = length === 0 ? '' : '?' + ',?'.repeat (length - 1)

		return routine instanceof DbFunction ? 
		
			this.getScalar (`SELECT ${routine.qName} (${args})`, params) : 

			this.do (`CALL ${routine.qName} (${args})`, params)

	}

	async insertRecord (name, record, options) {

		if (options.onlyIfMissing && !options.result) options.result = 'status'

		const call = await super.insertRecord (name, record, options)

		switch (options.result) {

			case 'status': return call.raw.rowCount === 1

			case 'record': return call.raw.rows [0]

			default: return call

		}

	}

	async upsert (name, data, options) {

		const params = this.lang.genUpsertParamsSql (name, data, options)
		
		const sql = params.pop ()
		
		await this.do (sql, params)

	}
	
	async putObjectStream (name, columns, options = {}) {

		const table = this.model.find (name), csv = this.toCsv ({table, columns})

		const os = await this.putBinaryStream (name, csv.columns.map (i => i.name), {...options, FORMAT: 'CSV'})

		const call = os [CALL]; csv [CALL] = call

		csv.on ('error',  () => os.destroy ())
		call.on ('error',  e => csv.emit ('error', e))
		os.on ('complete', () => csv.emit ('complete'))

		csv.pipe (os)

		return csv

	}

	async putBinaryStream (name, columns, options = {}) {

		const sql = this.lang.genCopyFromSql (name, columns, options)

		const call = this.call (sql, [], {...options, maxRows: 0, isPut: true})

		await call.exec ()

		const stream = call.raw; stream [CALL] = call

		stream.on ('error', cause => call.emit ('error', cause))

		stream.on ('finish', () => stream.emit ('complete'))

		stream.on ('complete', () => call.finish ())

		return stream

	}

	async getStreamOfExistingTables () {

		const {defaultSchema} = this.model

		if (!defaultSchema.schemaName) defaultSchema.schemaName = await this.getScalar ('SELECT CURRENT_SCHEMA ()')

		const rs = await this.getStream (this.lang.genSelectColumnsSql ())
		
		const xform = new DbTableXformerPg (this.model)
		
		rs.on ('error', x => xform.destroy (x))
		
		return rs.pipe (xform)

	}

}

module.exports = DbClientPg