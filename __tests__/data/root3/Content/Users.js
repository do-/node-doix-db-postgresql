module.exports = {

    select: async function () {
    
		return [{id: 1}]
        
    },

    getItem: async function () {

      const {request: {id}} = this
    	
    	if (isNaN (id)) throw Error ('Invalid id')

		  return {id}

    },
    
}