const Path = require ('path')
const {Application} = require ('doix')
const {DbModel} = require ('doix-db')
const {DbPoolPg, DbQueuesRouterPg, DbListenerPg} = require ('..')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()

const {Writable} = require ('stream')
const winston = require ('winston')
const logger = winston.createLogger({
	transports: [
//	  new winston.transports.Console (),
	  new winston.transports.Stream ({stream: new Writable ({write(){}})})
	]
})

const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}

const db = {
	connectionString: process.env.CONNECTION_STRING,
}

test ('bad', async () => {

	const app = new Application ({modules, logger, pools: {db: new DbPoolPg ({db, logger})}})

	expect (() => new DbQueuesRouterPg (app)).toThrow ()

	const ch = new DbQueuesRouterPg (app, {name: 'QR'})
	ch.router = {}

	expect (() => ch.getQueueName ()).toThrow ()
	expect (() => ch.getQueueName ('')).toThrow ()
	expect (() => ch.getQueueName ({})).toThrow ()
	expect (() => ch.getQueueName ({payload: 0})).toThrow ()
	expect (() => ch.getQueueName ({payload: ''})).toThrow ()

})

test ('queue: check', async () => {

	const app = new Application ({modules, logger, pools: {db: new DbPoolPg ({db, logger})}})

	const schemaName = 'doix_test_db_4'

	const pool = app.pools.get ('db')

	try {

		const model = new DbModel ({
			src: {
				schemaName,
				root: Path.join (__dirname, 'data', 'root4')
			},
			db: pool
		})

		const db = await pool.setResource (job, 'db')

		try {

			await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)
			await db.do (`SET SCHEMA '${schemaName}'`)
		
			model.loadModules ()
			
			const plan = db.createMigrationPlan ()
		
			await plan.loadStructure ()
			plan.inspectStructure ()
		
			await db.doAll (plan.genDDL ())

			const view = model.find ('q_1'), {queue} = view

			const a_in = [1, 2]

			await db.insert ('tb_1', a_in.map (id => ({id})))

			const a_out = await new Promise ((ok, fail) => {

				const a = []

			 	queue.on ('job-end',   job => a.push (job.result))
			 	queue.on ('job-error', job => fail (job.error))
			 	queue.on ('job-next',   () => {if (queue.pending.size === 0) ok (a)})

			 	queue.check ()

			})

			expect (a_out).toStrictEqual (a_in)

			await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)

		}
		finally {

			await db.release ()

		}
		
	}
	finally {

		await pool.pool.end ()

	}

})


test ('queue: listener', async () => {

	const dbl = new DbListenerPg ({channel: 'hotline', db, logger})

	const app = new Application ({modules, logger, pools: {
		db: new DbPoolPg ({db, logger}),
		db2: new DbPoolPg ({db: {connectionString: ''}, logger}),
	}})

	const pool = app.pools.get ('db')

	const a = [], ch = new DbQueuesRouterPg (app, {
		name: 'QR',
		on: {
			'job-end': job => a.push (job.result),
			'error': []
		}
	})

	const schemaName = 'doix_test_db_4'

	try {

		const model = new DbModel ({
			src: {
				schemaName,
				root: Path.join (__dirname, 'data', 'root4')
			},
			db: pool
		})

		const db = await pool.setResource (job, 'db')

		try {

			await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)
			await db.do (`SET SCHEMA '${schemaName}'`)

			model.loadModules ()
			
			const plan = db.createMigrationPlan ()
		
			await plan.loadStructure ()
			plan.inspectStructure ()
		
			await db.doAll (plan.genDDL ())

			const view = model.find ('q_1'), {queue} = view

			const fetch = async () => new Promise ((ok, fail) => {
				queue.on ('job-error', job => fail (job.error))
				queue.on ('job-next', () => queue.pending.size ? null : ok (a))
			})
			
			await db.insert ('tb_1', {id: 2})
			await db.insert ('tb_1', {id: 0})

			dbl.add (new DbQueuesRouterPg (app, {test: _ => false, name: 'QRF'}))
			dbl.add (ch)

			expect (() => ch.getQueue ({payload: '    '})).toThrow ('not found')
			expect (() => ch.getQueue ({payload: 'tb_1'})).toThrow ('Not a queue')

			expect (ch.router).toBe (dbl)

			await dbl.listen ()

			expect (await fetch ()).toStrictEqual ([0, 2])
			a.length = 0 
			
			await db.insert ('tb_1', {id: 3})
			await db.insert ('tb_1', {id: 1})

			expect ((await fetch ()).sort ()).toStrictEqual ([1, 3])

			expect (parseInt (await db.getScalar ('SELECT COUNT(*) FROM tb_1'))).toBe (0)

			await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)


		}
		finally {

			await db.release ()

		}
		
	}
	finally {

		await pool.pool.end ()
		await dbl.close ()

	}

})
