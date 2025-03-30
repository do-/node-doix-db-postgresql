const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

pool.logger = job.logger

afterAll(async () => {

	await pool.pool.end ()

})

test ('e7707', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')
				
		await expect (db.do ('...')).rejects.toThrow ()
		await expect (db.getArray ('SELECT 1 AS id', [], {maxRows: -1})).rejects.toThrow ()
		await expect (db.getArray ('SELECT 1 AS id', [], {maxRows: -Infinity})).rejects.toThrow ()
		await expect (db.getArray ('SELECT 1 AS id', [], {rowMode: 'whatever'})).rejects.toThrow ()

	}
	finally {

		await db.release ()

	}
	
})

test ('getArray 1', async () => {
	
	try {

		var db = await pool.setResource (job, 'db')

		const a = await db.getArray ('SELECT 1::int4 AS id')

		expect (a).toStrictEqual ([{id: 1}])

		expect (a[Symbol.for ('columns')] [0].type).toBe ('INT4')

	}
	finally {

		await db.release ()

	}
	
})

test ('getArray 1 array', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')

		const a = await db.getArray ('SELECT 1 AS id', [], {rowMode: 'array'})

		expect (a).toStrictEqual ([[1]])

	}
	finally {

		await db.release ()

	}
	
})

test ('getArray 1 scalar', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')

		const a = await db.getArray ('SELECT 1 AS id', [], {rowMode: 'scalar'})

		expect (a).toStrictEqual ([1])

	}
	finally {

		await db.release ()

	}
	
})

test ('drop create insert select', async () => {

	const dst = 'my_table', id = Math.floor (Math.random (10))

	try {

		var db = await pool.setResource (job, 'db')

		const schemaName = 'doix_test_db_5'
		await db.do (`DROP SCHEMA IF EXISTS ${schemaName} CASCADE`)
		await db.do (`CREATE SCHEMA ${schemaName}`)
		await db.do (`SET SCHEMA '${schemaName}'`)

		for (const sql of [

			`DROP TABLE IF EXISTS ${dst}`,

			`CREATE TABLE ${dst} (id INT)`,

		]) await db.do (sql)
		
		await db.do (`INSERT INTO ${dst} (id) VALUES (?)`, [id])

		const a = await db.getArray (`SELECT * FROM ${dst} WHERE id = ?`, [id])

		expect (a).toStrictEqual ([{id}])

	}
	finally {

		await db.release ()

	}
	
})


test ('1001', async () => {
	
	try {
	
		var db = await pool.setResource (job, 'db')
		
		const sql = 'SELECT * FROM generate_series (?::int, ?) id'
		
		const get = async (n, o = {}) => db.getArray (sql, [1, n], o)
		
		expect (await get (1000)).toHaveLength (1000)
		expect (await get (1001, {isPartial: true})).toHaveLength (1000)
		expect (await get (1001, {maxRows: 2000})).toHaveLength (1001)

		await get (1001)

	}
	catch (x) {
	
		expect (x).toBeInstanceOf (Error)
	
	}
	finally {

		await db.release ()

	}
	
})