module.exports = {

    select_users: async function () {
    
		return [{id: 1}]
        
    },

    get_item_of_users: async function () {

    	const {rq: {id}} = this
    	
    	if (isNaN (id)) throw Error ('Invalid id')

		return {id}

    },
    
}