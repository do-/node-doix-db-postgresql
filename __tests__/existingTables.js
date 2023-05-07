const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})


afterAll(async () => {

	await pool.pool.end ()

})

test ('error', async () => {

	let xxx
	
	try {
	
		var db = await pool.toSet (job, 'db'), backup = db.lang.genSelectColumnsSql
		
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
	
		var db = await pool.toSet (job, 'db')
		
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
		expect (t.columns.label.type).toBe ('text')
		expect (t.columns.salary.size).toBe (10)
		expect (t.columns.salary.scale).toBe (2)

		await db.do (`DROP SCHEMA ${dbName} CASCADE`)

	}
	finally {

		await db.release ()

	}
	
})
