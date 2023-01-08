const {Client} = require ('pg')
const DbClientPg = require ('../lib/DbClientPg')

let db = {}
beforeAll(async () => {
	const client = new Client ({connectionString: 'postgresql://postgres:z@localhost:5432/db_empty'})
	await client.connect()
	client.release = client.end
	db = new DbClientPg (client)
	db.on ('error', x => {throw x})
})

afterAll(async () => {
	await db.release ()
})

test ('SELECT 1', async () => {
	let r = await db.do ('SELECT 1 AS id')
	expect (r.rows [0]).toStrictEqual ({id: 1})
})

test ('SELECT FROM voc_some', async () => {
	for (let s of [
		{sql: `DROP TABLE IF EXISTS voc_some`},
		{sql: `CREATE TABLE voc_some (id int PRIMARY KEY, code TEXT, label TEXT)`},
		{sql: `INSERT INTO voc_some (id, code, label) VALUES (1, 'foo', 'barbar')`},
	]) {
		await db.do (s.sql, s.params)
	}
	
	let r = await db.do ('SELECT id, code, label FROM voc_some')
	expect (r.rows).toStrictEqual ([{id: 1, code: 'foo', label: 'barbar'}])
})
