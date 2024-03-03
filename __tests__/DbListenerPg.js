const pg = require ('pg')
const Path = require ('path')
const {Application, ConsoleLogger} = require ('doix')
const {DbListenerPg, DbChannelPg} = require ('..')

const logger = {log: _ => {}}//new ConsoleLogger ()
const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}
const app = new Application ({modules, logger})

const db = {
	connectionString: process.env.CONNECTION_STRING,
}

test ('bad', () => {

	expect (() => new DbChannelPg (app)).toThrow ()	
	expect (() => new DbChannelPg (app, {})).toThrow ()	
	expect (() => new DbChannelPg (app, {name: 1})).toThrow ()	
	expect (() => new DbChannelPg (app, {name: ''})).toThrow ()	
	expect (() => new DbListenerPg ({db})).toThrow ()	

})

test ('basic', async () => {

	const dbl = new DbListenerPg ({db, logger})

	const [result] = await Promise.all ([

		new Promise ((ok, fail) => {

			dbl.add (new DbChannelPg (app, {
				name: 'hotline',
				on: {
					start: function () {
						this.rq = JSON.parse (this.notification.payload)
					},
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
			await client.query (`NOTIFY hotline, '{"type":"users","id":2}'`)
			await client.end()
		
		})()
	
	])

	await dbl.close ()

	expect (result).toStrictEqual ({id: 2})

})