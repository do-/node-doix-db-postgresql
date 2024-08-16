const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')
const {DbModel} = require ('doix-db')
const DbTableXformerPg = require ('../lib/DbTableXformerPg.js')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

afterAll(async () => {

	await pool.pool.end ()

})

test ('xform fail', () => {

	const xf = new DbTableXformerPg (new DbModel ({db: pool}))

	let x

	const cb = (err, data) => {

		if (err) return x = err

		throw data

	}

	xf._transform ({}, null, cb)
	
	expect (x).toBeInstanceOf (Error)

})

test ('error', async () => {

	let xxx
	
	try {
	
		new DbModel ({db: pool})

		var db = await pool.setResource (job, 'db'), backup = db.lang.genSelectColumnsSql
		
		db.lang.genSelectColumnsSql = () => 'noSQL'

		const a = [], ts = await db.getStreamOfExistingTables ()

		for await (const t of ts) a.push (t)

	}
	catch (x) {

		xxx = x

	}
	finally {
	
		db.lang.genSelectColumnsSql = backup

		await db.release ()

	}
	
	expect (xxx).toBeDefined ()
	
})

test ('basic', async () => {

	const dbName = 'doix_test_db_2'
	
	try {

		new DbModel ({db: pool, src: {schemaName: dbName}})
	
		var db = await pool.setResource (job, 'db')
		
		await db.do (`DROP SCHEMA IF EXISTS ${dbName} CASCADE`)
		await db.do (`CREATE SCHEMA ${dbName}`)
		await db.do (`SET SCHEMA '${dbName}'`)

		await db.do ('CREATE TABLE users (id int, label text, salary decimal(10, 2) DEFAULT 0, PRIMARY KEY (id))')

		const a = [], ts = await db.getStreamOfExistingTables ()

		for await (const t of ts) a.push (t)

		expect (a).toHaveLength (1)
		
		const [t] = a

		expect (t.pk).toStrictEqual (['id'])
		expect (Object.keys (t.columns).sort ()).toStrictEqual (['id', 'label', 'salary'])
		expect (t.columns.label.typeDim).toBe ('TEXT')
		expect (t.columns.salary.typeDim).toBe ('NUMERIC(10,2)')

		await db.do (`DROP SCHEMA ${dbName} CASCADE`)

	}
	finally {

		await db.release ()

	}
	
})
