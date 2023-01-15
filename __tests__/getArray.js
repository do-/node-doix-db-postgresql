const MockJob = require ('./lib/MockJob.js'), job = new MockJob ()
const {DbClientPg, DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

afterAll(async () => {

	await pool.pool.end ()

})

test ('e7707', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')
				
		await expect (db.do ('...')).rejects.toThrow ()
		await expect (db.getArray ('SELECT 1 AS id', [], {maxRows: -1})).rejects.toThrow ()
		await expect (db.getArray ('SELECT 1 AS id', [], {maxRows: Infinity})).rejects.toThrow ()

	}
	finally {

		await db.release ()

	}
	
})

test ('getArray 1', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const a = await db.getArray ('SELECT 1 AS id')

		expect (a).toStrictEqual ([{id: 1}])

	}
	finally {

		await db.release ()

	}
	
})

test ('getArray 1 array', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const a = await db.getArray ('SELECT 1 AS id', [], {rowMode: 'array'})

		expect (a).toStrictEqual ([[1]])

	}
	finally {

		await db.release ()

	}
	
})

test ('drop create insert select', async () => {

	const dst = 'my_table', id = Math.floor (Math.random (10))

	try {

		var db = await pool.toSet (job, 'db')

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
	
		var db = await pool.toSet (job, 'db')

		const a1000 = await db.getArray ('SELECT * FROM generate_series (?::int, ?) id', [1, 1000])
		
		expect (a1000).toHaveLength (1000)

		const a1001 = await db.getArray ('SELECT * FROM generate_series (?::int, ?) id', [1, 1001], {maxRows: 2000})

		expect (a1001).toHaveLength (1001)

		await db.getArray ('SELECT * FROM generate_series (?::int, ?) id', [1, 1001])

	}
	catch (x) {
	
		expect (x).toBeInstanceOf (Error)
	
	}
	finally {

		await db.release ()

	}
	
})
