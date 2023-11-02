const PgQueryStream = require ('pg-query-stream')
const PgCopyStream  = require ('pg-copy-streams')
const PgCursor      = require ('pg-cursor')
const {MAP_TYPE_ID_2_NAME} = require ('./DbLangPg.js')
const {DbClient} = require ('doix-db')
const DbTableXformerPg = require ('./DbTableXformerPg.js')

class DbClientPg extends DbClient {

	constructor (raw) {
	
		super ()

		this.raw = raw
	
	}
	
	async release () {
	
		this.raw.release ()
	
	}

	async exec (call) {

		const {raw} = this, {sql, params, options} = call, {checkOverflow, maxRows} = options, o = call.objectMode ? {} : {rowMode: 'array'}

		switch (maxRows) {

			case 0        : return call.raw = await raw.query ({text: sql, values: params})

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
	
	async upsert (name, data, options) {

		const params = this.lang.genUpsertParamsSql (name, data, options)
		
		const sql = params.pop ()
		
		await this.do (sql, params)

	}

	async putStream (name, columns, options = {}) {

		const sql = this.lang.genCopyFromSql (name, columns, options)

		this.emit ('start', {sql, params: []})

		const stream = await this.raw.query (PgCopyStream.from (sql))

		stream.on ('error', cause => this.emit ('error', cause))

		stream.on ('finish', () => this.emit ('finish'))

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