const pg = require ('pg')
const Path = require ('path')
const {Application} = require ('doix')
const {DbListenerPg, DbServicePg} = require ('..')

const {Writable} = require ('stream')
const winston = require ('winston')
const logger = winston.createLogger({
	transports: [
//	  new winston.transports.Console (),
	  new winston.transports.Stream ({stream: new Writable ({write(){}})})
	],
	format: winston.format.combine (
		winston.format.timestamp ({format: 'YYYY-MM-DD[T]hh:mm:ss.SSS'}),
		winston.format.printf ((i => `${i.timestamp} ${i.event} ${i.id} ${i.event === 'finish' ? i.elapsed + ' ms' : i.message}${i.details ? ' ' + JSON.stringify (i.details) : ''}`))
	),
})

const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}
const app = new Application ({modules, logger})

const db = {
	connectionString: process.env.CONNECTION_STRING,
}

test ('bad', () => {

	expect (() => new DbListenerPg ({})).toThrow ('channel not set')
	expect (() => new DbListenerPg ({channel: ''})).toThrow ('channel not set')
	expect (() => new DbListenerPg ({channel: 11})).toThrow ('string')

	expect (() => new DbListenerPg ({channel: 'hotline', db})).toThrow ('`logger` not set')

})

test ('basic', async () => {

	const dbl = new DbListenerPg ({channel: 'coolline', db, logger})

	const result = new Set ()
	
	await Promise.all ([

		new Promise ((ok, fail) => {

			dbl.add (
				new DbServicePg (app, {
					name: 'never',
					test: _ => false,
					on: {
						end: function () {
							ok (result.add (this.result.id))
						},
						error: function () {
							fail (this.error)
						},
					},
				})
			)

			dbl.add (
				new DbServicePg (app, {
					name: 'always',
					test: _ => true,
					on: {
						end: function () {
							ok (result.add (-this.result.id))
						},
						error: function () {
							fail (this.error)
						},
					},
				})
			)
		
			dbl.add ({})

		}),

		(async () => {

			await dbl.listen ()

			const client = new pg.Client (db)
			await client.connect ()
			await client.query (`NOTIFY noline,   '{"type":"users","id":0}'`)
			await client.query (`NOTIFY coolline, '{"type":"users","id":2}'`)
			await client.end()
		
		})()
	
	])

	await dbl.close ()

	expect ([...result.values ()].sort ()).toStrictEqual ([-2])

})
/*
test ('failing', async () => {

	const dbl = new DbListenerPg ({channel: 'hotline', db, logger})

	await expect (Promise.all ([

		new Promise ((ok, fail) => {

			dbl.add (new DbServicePg (app, {
				name: 'nope',
				on: {					
					end: function () {
						ok (this.result)
					},
					error: function () {
						fail (this.error)
					},
				},
			}))		
		
			dbl.add ({})

		}),

		(async () => {

			await dbl.listen ()

			const client = new pg.Client (db)
			await client.connect ()
			await client.query (`NOTIFY hotline, '{"type":"users","id":"two"}'`)
			await client.end()
		
		})()
	
	])).rejects.toThrow ()

	await dbl.close ()

})
*/