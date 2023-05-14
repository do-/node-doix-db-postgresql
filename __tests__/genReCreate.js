const {DbLangPg} = require ('..'), lang = new DbLangPg ()

test ('error', () => {

	expect (() => lang.genReCreate ({})).toThrow ()

})
