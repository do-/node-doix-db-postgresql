const Path = require ('path')
const {Application
//	, ConsoleLogger
} = require ('doix')
const {DbModel, DbQueue} = require ('doix-db')
const {DbPoolPg, DbListenerPg, DbQueuesRouterPg} = require ('..')
const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()

const logger = 
{log: _ => {}}
//new ConsoleLogger ()
const modules = {dir: {root: Path.join (__dirname, 'data', 'root3')}}

const db = {
	connectionString: process.env.CONNECTION_STRING,
}

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

		const db = await pool.toSet (job, 'db')

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
				queue.on ('job-finished', () => queue.pending.size ? null : ok (a))

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

test ('router getQueueName fails', async () => {

	const app = new Application ({modules, logger, pools: {db: new DbPoolPg ({db, logger})}})

	const ch = new DbQueuesRouterPg (app)
	ch.router = {}

	expect (() => ch.getQueueName ()).toThrow ()
	expect (() => ch.getQueueName ('')).toThrow ()
	expect (() => ch.getQueueName ({})).toThrow ()
	expect (() => ch.getQueueName ({payload: 0})).toThrow ()
	expect (() => ch.getQueueName ({payload: ''})).toThrow ()

})

test ('queue: listener', async () => {

	const dbl = new DbListenerPg ({channel: 'hotline', db, logger})

	const app = new Application ({modules, logger, pools: {
		db: new DbPoolPg ({db, logger}),
		db2: new DbPoolPg ({db: {connectionString: ''}, logger}),
	}})

	const pool = app.pools.get ('db')

	const a = [], ch = new DbQueuesRouterPg (app, {
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

		const db = await pool.toSet (job, 'db')

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
				queue.on ('job-finished', () => queue.pending.size ? null : ok (a))
			})
			
			await db.insert ('tb_1', {id: 2})
			await db.insert ('tb_1', {id: 0})

			dbl.add (new DbQueuesRouterPg (app, {test: _ => false}))
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