const {DbLangPg} = require ('..')

test ('error', () => {

	const lang = new DbLangPg ()

	expect (() => [...lang.genDDL ()]).toThrow ()	

})

test ('empty', () => {

	const lang = new DbLangPg ()
	
	lang.migrationPlan = {toDo: new Map ()}

	expect ([...lang.genDDL ()]).toStrictEqual ([])

})