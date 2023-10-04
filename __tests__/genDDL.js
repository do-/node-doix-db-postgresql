const {DbModel} = require ('doix-db')
const {DbPoolPg} = require ('..')

const pool = new DbPoolPg ({
	db: {
		connectionString: process.env.CONNECTION_STRING,
	},
})


test ('error', () => {

	const model = new DbModel ({db: pool}), {lang} = model

	expect (() => [...lang.genAlter (0, 0)]).toThrow ()	

	expect (() => [...lang.genDDL ()]).toThrow ()	

	expect (lang.genColumnDefault ({default: 'NULL'})).toBe ('NULL')	

})

test ('empty', () => {

	const model = new DbModel ({db: pool}), {lang} = model
	
	lang.migrationPlan = {toDo: new Map ()}

	expect ([...lang.genDDL ()]).toStrictEqual ([])

})