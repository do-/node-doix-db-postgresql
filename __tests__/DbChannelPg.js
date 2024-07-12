const Path = require ('path')
const {Application, ResourcePool} = require ('doix')
const {DbModel} = require ('doix-db')
const {DbChannelPg, DbListenerPg, DbPoolPg, DbQueuePg} = require ('..')

const logger = {log: _ => {}}//new ConsoleLogger ()
const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}

const schemaName = 'doix_test_db_4'
const db = {
	connectionString: process.env.CONNECTION_STRING,
}

const app = new Application ({modules, logger, pools: {
	db: new DbPoolPg ({db, logger}),
	db2: new ResourcePool (),
}})

test ('bad', () => {

	expect (() => new DbChannelPg (app)).toThrow ()	
	expect (() => new DbChannelPg (app, {})).toThrow ()	
	expect (() => new DbChannelPg (app, {name: 1})).toThrow ()	
	expect (() => new DbChannelPg (app, {name: ''})).toThrow ()	

	const ch = new DbChannelPg (app, {name: 'hotline'})
	ch.router = {}

})

test ('getQueue', async () => {

	app.pools.get ('db2').wrapper = Object

	const pool = app.pools.get ('db')

	try {

		const model = new DbModel ({
			src: {
				schemaName,
				root: Path.join (__dirname, 'data', 'root4')
			},
			db: pool
		})
	
		model.loadModules ()
	
		const dbl = new DbListenerPg ({db, logger})
	
		const ch = new DbChannelPg (app, {name: 'hotline'})
	
		dbl.add (ch)
	
		expect (ch.router).toBe (dbl)
		expect (ch.getQueue ({})).toBeUndefined ()
		expect (ch.getQueue ({payload: 'q_1'})).toBeInstanceOf (DbQueuePg)

	}
	finally {

		await pool.pool.end ()

	}

})