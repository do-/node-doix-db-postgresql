const PgQueryStream = require ('pg-query-stream')
const PgCopyStream  = require ('pg-copy-streams')
const PgCursor      = require ('pg-cursor')
const {MAP_TYPE_ID_2_NAME} = require ('./DbLangPg.js')
const {DbClient, DbFunction, DbProcedure} = require ('doix-db')
const DbTableXformerPg = require ('./DbTableXformerPg.js')
const UUID = Symbol ('UUID')
const CALL = Symbol.for ('doixDbCall')

class DbClientPg extends DbClient {

	constructor (raw) {
	
		super ()

		this.raw = raw

		if (UUID in raw) this.uuid = raw [UUID]; else raw [UUID] = this.uuid
	
	}

	getCallLoggingEvents () {

		return {

			...super.getCallLoggingEvents (),

			notice: {
				level: 'info',
				message: n => n.message,
				details: ({where}) => ({where}),
			},

		}

	}

	async begin () {
	
		await this.do ('BEGIN')

		this.txn = {}
	
	}

	async commit () {
	
		if (this.isAutoCommit ()) return

		await this.do ('COMMIT')

		this.txn = null
	
	}

	async rollback () {
	
		if (this.isAutoCommit ()) return

		await this.do ('ROLLBACK')

		this.txn = null
	
	}

	async terminate () {

		const {job, name, pool} = this

		try {

			var db = await pool.setResource (job, name)

			await db.do ('SELECT pg_terminate_backend (?)', [this.raw.processID])

		}
		finally {

			this.raw.release (true)

			db.raw.release ()

		}

	}
	
	async release () {

		const {activeQuery} = this.raw
		
		if (activeQuery) {

			const {_writableState} = activeQuery; if (!_writableState || !_writableState.destroyed) return this.terminate ()

			await new Promise (ok => activeQuery.on ('close', ok))

		}
			
		if (!this.isAutoCommit ()) {

			if (this.job.error) {

				await this.rollback ()

			}
			else {

				await this.commit ()

			}

		}
	
		this.raw.release ()

		this.emit ('released')
	
	}

	isAutoCommit () {

		return this.txn == null

	}

	async exec (call) {	

		{

			const onNotice = m => call.emit ('notice', m)
			
			const {raw} = this

			raw.on ('notice', onNotice)

			call.on ('finish', () => raw.off ('notice', onNotice))

		}

		const {raw} = this, {sql, params, options} = call, {checkOverflow, maxRows} = options, o = call.objectMode ? {} : {rowMode: 'array'}

		try {

			switch (maxRows) {

				case 0 : return call.raw = await raw.query (
					
					options.isPut ? PgCopyStream.from (sql)
					
					: {text: sql, values: params}
					
				)

				case Infinity :

					return call.rows = await raw.query (
						this.lang.isCopyStatement (sql) ? PgCopyStream.to (sql) :
						new PgQueryStream (sql, params, o)
					)

				default       :

					const cursor = call.raw = await raw.query (new PgCursor (sql, params, o))

					await new Promise ((ok, fail) => {
		
						cursor.read (checkOverflow ? maxRows + 1: maxRows, (error, rows) => {
		
							try {

								if (error) throw error

								if (checkOverflow && rows.length > maxRows) throw Error ('Result set overflow, maxRows = ' + maxRows)

								call.columns = cursor._result.fields.map (i => {i.type = MAP_TYPE_ID_2_NAME.get (i.dataTypeID); return i})
			
								call.rows = rows

								cursor.close ().then (ok, fail)

							}
							catch (x) {

								const carp = () => fail (x)

								cursor.close ().then (carp, carp)

							}
							
						})
				
					}

				)

			}
			
		}
		catch (cause) {

			const {message} = cause, {sql, params, options} = call, o = {message, call: {sql, params, options}}
				
//			for (const k in cause) if (err [k] !== undefined) o [k] = err [k]

			{

				if (cause.code == '23505' && this.model) {
					
					const {schema, table, constraint} = cause

					for (const {schemaName, map} of this.model.schemata.values ()) if (schemaName === schema && map.has (table)) {

						const {keys} = map.get (table); for (const k of Object.values (keys)) if (k !== null && k.localName === constraint && 'message' in k)  

							o.message = k.message (cause)

					}

				}

			}

			throw Error (JSON.stringify (o), {cause})

		}

	}
	
	async invoke (name, params = []) {

		const routine = this.model.find (name)
		, isUD = routine != null
		, isUDF = isUD && routine instanceof DbFunction
		, isUDP = isUD && routine instanceof DbProcedure

		if (isUD) {

			if (isUDF || isUDP) {

				name = routine.qName

			}
			else {

				throw Error ('Not a routine to be called: ' + name)

			}

		}

		const {length} = params, args = length === 0 ? '' : '?' + ',?'.repeat (length - 1)

		if (isUDP) return this.do (`CALL ${routine.qName} (${args})`, params)
		
		return this.getScalar (`SELECT ${name} (${args})`, params)

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

		const sql = this.lang.genCopyFromSql (name, columns, options), cause = new Error ('(tracking error)')

		const call = this.call (sql, [], {...options, maxRows: 0, isPut: true})

		await call.exec ()

		const stream = call.raw; stream [CALL] = call

		stream.on ('error', err => {err.cause = cause; call.emit ('error', err)})

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

	async peek (queue) {

		return this.getObject (this.lang.genPeekSql (queue), [], {notFound: null})

	}

}

module.exports = DbClientPg