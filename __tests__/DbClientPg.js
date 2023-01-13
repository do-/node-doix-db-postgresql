//const {Client} = require ('pg')

const EventEmitter = require ('events')
const {DbClientPg, DbPoolPg} = require ('..'), {normalizeSQL} = DbClientPg

const job = new EventEmitter ()
job.uuid = '00000000-0000-0000-0000-000000000000'
job.logger = {log: ({message, level}) => {if (false) console.log (level + ' ' + message)}}

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})

afterAll(async () => {

	await pool.pool.end ()

})

test ('normalizeSQL', () => {

	expect (normalizeSQL ('SELECT 1')).toBe ('SELECT 1')

	expect (normalizeSQL ('SELECT * FROM t WHERE id = ?')).toBe ('SELECT * FROM t WHERE id = $1')
	
	expect (normalizeSQL ('SELECT * FROM t WHERE id = ? AND label LIKE ?')).toBe ('SELECT * FROM t WHERE id = $1 AND label LIKE $2')

	expect (normalizeSQL ('SELECT * FROM t WHERE id::jsonb ? ? AND label LIKE ?')).toBe ('SELECT * FROM t WHERE id::jsonb ? $1 AND label LIKE $2')

})

test ('e7707', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')
				
		await expect (db.do ('...')).rejects.toThrow ()

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

test ('getArray 1 into', async () => {
	
	try {
	
		var db = await pool.toSet (job, 'db')

		const into = [1]	
	
		const a = await db.getArray ('SELECT 1 AS id', [], {into})

		expect (a).toBe (into)

		expect (a).toStrictEqual ([1, {id: 1}])

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