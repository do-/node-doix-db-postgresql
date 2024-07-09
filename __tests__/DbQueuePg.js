const Path = require ('path')
const {Application
//	, ConsoleLogger
} = require ('doix')
const {DbModel} = require ('doix-db')
const {DbPoolPg, DbQueuePg, DbListenerPg, DbChannelPg} = require ('..')
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

			expect (() => new DbQueuePg (app, {view})).toThrow ('order')

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

test ('queue: listener', async () => {

	const dbl = new DbListenerPg ({db, logger})
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

			expect (() => new DbQueuePg (app, {view})).toThrow ('order')
			
			await db.insert ('tb_1', {id: 2})

			const ch = new DbChannelPg (app, {name: 'hotline'})
			dbl.add (ch)
			await dbl.listen ()
			
			await db.insert ('tb_1', {id: 1})

			const a_out = await new Promise ((ok, fail) => {

				const a = []

				queue.on ('job-end',   job => a.push (job.result))
				queue.on ('job-error', job => fail (job.error))
				queue.on ('job-finished', () => queue.pending.size ? null : ok (a))

			})

			expect (a_out).toStrictEqual ([1, 2])

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
