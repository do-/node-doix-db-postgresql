module.exports = {

    label: 'Table 1',

    columns: {
        id       : 'int    // PK',
        label    : 'text=on   // Human readable',
        amount   : 'decimal(10,2)=0 // Some money',
    },
    
    pk: 'id',

}