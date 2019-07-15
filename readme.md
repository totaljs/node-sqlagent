# A very helpful ORM for node.js

[![Professional Support](https://www.totaljs.com/img/badge-support.svg)](https://www.totaljs.com/support/) [![Chat with contributors](https://www.totaljs.com/img/badge-chat.svg)](https://messenger.totaljs.com) [![NPM version][npm-version-image]][npm-url] [![NPM downloads][npm-downloads-image]][npm-url] [![MIT License][license-image]][license-url]

- installation `$ npm install sqlagent`

---

- for PostgreSQL `$ npm install pg`
- for MySQL `$ npm install mysql`
- for MS SQL Server `$ npm install mssql`
- for MongoDB `$ npm install mongodb`

---

- Currently supports __PostgreSQL__, __MySQL__, __SQL Server__ and __MongoDB__
- Simple and powerful
- Best use with [Total.js - web framework for Node.js](https://www.totaljs.com)

__IMPORTANT__:

- the code is executed as is added
- `rollback` is executed automatically when is the transaction enabled
- SQL Server: pagination works only in `SQL SERVER >=2012`
- `SqlBuilder` is a global object
- `undefined` values are skipped

## Initialization

### Basic initialization

#### PostgreSQL

```javascript
// Example: postgresql://user:password@127.0.0.1/database
var Agent = require('sqlagent/pg').connect('connetion-string-to-postgresql');

/*
// It's executed when the datbase returns an unexpected error
Agent.error = function(err, type, query) {

};
*/

// Agent() returns new instance of SQL Agent
var sql = Agent();
```

__Additional configuration__:

```
postgresql://user:password@127.0.0.1/database?native=true&ssl=true
```

- `native` {Boolean} enables PG C native binding (faster than JavaScript binding, default: `false`)
- `ssl` {Boolean} enables SSL (default: `false`)
- `max` {Number} max. pools (default: `20`)
- `min` {Number} min. pools (default: `4`)
- `idleTimeoutMillis` {Number} idle timeout (default: `1000`)

#### MySQL

```javascript
// Example: mysql://user:password@127.0.0.1/database
var Agent = require('sqlagent/mysql').connect('connetion-string-to-mysql');
var sql = new Agent();
```

#### SQL Server (MSSQL)

```javascript
// Example: mssql://user:password@127.0.0.1/database
// Example with name of instance: mssql://user:password@localhost_SQLEXPRESS/database
var Agent = require('sqlagent/sqlserver').connect('connetion-string-to-mssql');
var sql = new Agent();
```

#### MongoDB

```javascript
// Example: mongodb://user:password@127.0.0.1/database
var Agent = require('sqlagent/mongodb').connect('connetion-string-to-mongodb');
var nosql = new Agent();
```

### Initialization for Total.js

Create a definition file:

```javascript
// Below code rewrites total.js database prototype
require('sqlagent/pg').init('connetion-string-to-postgresql', [debug]); // debug is by default: false
require('sqlagent/mysql').init('connetion-string-to-mysql', [debug]); // debug is by default: false
require('sqlagent/sqlserver').init('connetion-string-to-sqlserver', [debug]); // debug is by default: false
require('sqlagent/mongodb').init('connetion-string-to-mongodb', [debug]); // debug is by default: false
```

Usage:

```javascript
// When you use RDMBS:
// var sql = DATABASE([ErrorBuilder]);
var sql = DATABASE();
// sql === SqlAgent

// +v9.9.6 enable debugging
sql.debug = true;

// When you use MongoDB:
// var nosql = DATABASE([ErrorBuilder]);
var nosql = DATABASE();
// nosql === SqlAgent
```

### IMPORTANT

In order for mysql to return Boolean values please set the data type in db to BIT(1) and use bellow code for initialization.
```javascript
var Agent = require('sqlagent/mysql').connect({
    host: "localhost",
    user: "root",
    password: "",
    database: "test",
    typeCast: function castField( field, useDefaultTypeCasting ) {
        if ( ( field.type === "BIT" ) && ( field.length === 1 ) ) {
            var bytes = field.buffer();
            return( bytes[ 0 ] === 1 );
        }
        return( useDefaultTypeCasting() );
    }
});
var sql = new Agent();
```

## Usage

### Select

```plain
instance.select([name], table)
```

- `name` (String) is an identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- __returns__ SqlBuilder

```javascript
sql.select('users', 'tbl_user').make(function(builder) {
    builder.where('id', '>', 5);
    builder.page(10, 10);
});

sql.select('orders', 'tbl_order').make(function(builder) {
    builder.where('isremoved', false);
    builder.page(10, 10);
    builder.fields('amount', 'datecreated');
});

sql.select('products', 'tbl_products').make(function(builder) {
    builder.between('price', 30, 50);
    builder.and();
    builder.where('isremoved', false);
    builder.limit(20);
    builder.fields('id', 'name');
});

sql.exec(function(err, response) {
    console.log(response.users);
    console.log(response.products);
    console.log(response.admin);
});
```

### Push (only for MongoDB)

```plain
instance.push([name], collection, fn(collection, callback(err, response))
```

```javascript
sql.push('users', 'users', function(collection, callback) {

    var $group = {};
    $group._id = {};
    $group._id = '$category';
    $group.count = { $sum: 1 };

    var $match = {};
    $match.isremoved = false;

    var pipeline = [];
    pipeline.push({ $match: $match });
    pipeline.push({ $group: $group });

    collection.aggregate(pipeline, callback);
});

// OR

sql.push('users', 'users', function(collection, callback) {
    collection.findOne({ name: 'Peter' }, { name: 1, age: 1 }).toArray(callback);
});
```

### Listing

```plain
instance.listing([name], table)
```

- `name` (String) is an identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- __returns__ SqlBuilder

```javascript
sql.listing('users', 'tbl_user').make(function(builder) {
    builder.where('id', '>', 5);
    builder.page(10, 10);
});

sql.exec(function(err, response) {

    // users will contain:
    // .count --> count of all users according to the filter
    // .items --> selected items
    // .page  --> a page number (+v11.0.0)
    // .pages --> page count (+v11.0.0)
    // .limit --> items limit per page (+v11.0.0)

    console.log(response.users.count);
    console.log(response.users.items);
});
```


### Save

```plain
instance.save([name], table, isINSERT, prepare(builder, isINSERT));
```

```javascript
sql.save('user', 'tbl_user', somemodel.id === 0, function(builder, isINSERT) {

    builder.set('name', somemodel.name);

    if (isINSERT) {
        builder.set('datecreated', new Date());
        return;
    }

    builder.inc('countupdate', 1);
    builder.where('id', somemodel.id);
});
```

### Insert

```plain
instance.insert([name], table)
```

- `name` (String) is an identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- __returns__ if value is undefined then __SqlBuilder__ otherwise __SqlAgent__

```javascript
sql.insert('user', 'tbl_user').make(function(builder) {
    builder.set({ name: 'Peter', age: 30 });
});

sql.insert('log', 'tbl_logs').make(function(builder) {
    builder.set('message', 'Some log message.');
    builder.set('created', new Date());
});

sql.exec(function(err, response) {
    console.log(response.user); // response.user.identity (INSERTED IDENTITY)
    console.log(response.log); // response.log.identity (INSERTED IDENTITY)
});
```

__IMPORTANT__: `identity` works only with auto-increment in MS SQL SERVER.

### Update

```plain
instance.update([name], table)
```

- `name` (String) is an identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- __returns__ if value is undefined then __SqlBuilder__ otherwise __SqlAgent__

```javascript
sql.update('user1', 'tbl_user').make(function(builder) {
    builder.set({ name: 'Peter', age: 30 });
    builder.where('id', 1);
});

// is same as
sql.update('user2', 'tbl_user').make(function(builder) {
    builder.where('id', 1);
    builder.set('name', 'Peter');
    builder.set('age', 30);
});

sql.exec(function(err, response) {
    console.log(response.user1); // returns {Number} (count of changed rows)
    console.log(response.user2); // returns {Number} (count of changed rows)
});
```

### Delete

```plain
instance.delete([name], table)
instance.remove([name], table)
```

- `name` (String) is an identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- __returns__ SqlBuilder

```javascript
sql.remove('user', 'tbl_user').make(function(builder) {
    builder.where('id', 1);
});

sql.exec(function(err, response) {
    console.log(response.user); // returns {Number} (count of deleted rows)
});
```

### Query

```plain
instance.query([name], query)
```

- `name` (String) is an identificator for results, optional (default: internal indexer)
- `query` (String) SQL query
- `params` (Array) SQL additional params (each DB has own SQL implementation e.g. PG `WHERE id=$1`, MySQL `WHERE id=?`, etc.)
- __returns__ if params is undefined then __SqlBuilder__ otherwise __SqlAgent__

```javascript
sql.query('user', 'SELECT * FROM tbl_user').make(function(builder) {
    builder.where('id', 1);
});

sql.exec(function(err, response) {
    console.log(response.user);
});
```

### Aggregation

```plain
instance.count([name], table)
```

- __returns__ SqlBuilder

```javascript
var count = sql.count('users', 'tbl_user');
count.between('age', 20, 40);

sql.exec(function(err, response) {
    console.log(response.users); // response.users === number
});
```

---

```plain
instance.max([name], table, column)
instance.min([name], table, column)
instance.avg([name], table, column)
```

- __returns__ SqlBuilder

```javascript
var max = sql.max('users', 'tbl_user', 'age');
max.where('isremoved', false);

sql.exec(function(err, response) {
    console.log(response.users); // response.users === number
});
```

### Exists

```plain
instance.exists([name], table)
```

- __returns__ SqlBuilder

```javascript
var exists = sql.exists('user', 'tbl_user');
exists.where('id', 35);

sql.exec(function(err, response) {
    console.log(response.user); // response.user === Boolean (in correct case otherwise undefined)
});
```

### Compare

```plain
instance.compare([name], table, value, [keys])
```

- the module compares values between DB and `value`
- the response can be `false` or `{ diff: ['name'], record: Object, value: Object }`
- works with `sql.ifexists()` and `sql.ifnot()`
- __returns__ SqlBuilder

```javascript
var compare = sql.compare('user', 'tbl_user', { name: 'Peter', age: 33 });
// OR: var compare = sql.compare('user', 'tbl_user', { name: 'Peter', age: 33 }, ['name']); --> compares only name field
// OR: compare.fields('name', 'age'); --> compares these fields (if aren't defined "keys")

compare.where('id', 35);

sql.exec(function(err, response) {

    if (response.user) {
        // shows the property names which were changed
        console.log(response.user.diff);
    }

});
```

---

```plain
instance.max([name], table, column)
instance.min([name], table, column)
instance.avg([name], table, column) // doesn't work with Mongo
```

- __returns__ SqlBuilder

```javascript
var max = sql.max('users', 'tbl_user', 'age');
max.where('isremoved', false);

sql.exec(function(err, response) {
    console.log(response.users); // response.users === number
});
```

### Transactions

- doesn't work with MongoDB
- rollback is performed automatically

```javascript
sql.begin();
sql.insert('tbl_user', { name: 'Peter' });
sql.commit();
```

## Special cases

### How to set the primary key?

- doesn't work with MongoDB

```javascript
// instance.primary('column name') is same as instance.primaryKey('column name')

instance.primary('userid');
instance.insert('tbl_user', ...);

instance.primary('productid');
instance.insert('tbl_product', ...);

instance.primary(); // back to default "id"
```

- default `primary key name` is `id`
- works only in PostgreSQL because INSERT ... RETURNING __must have specific column name__

### How to use latest primary id value for relations?

```javascript
// primary key is id + autoincrement
var user = sql.insert('user', 'tbl_user');
user.set('name', 'Peter');

var address = sql.insert('tbl_user_address');
address.set('id', sql.$$);
address.set('country', 'Slovakia');

sql.exec();
```

### How to use latest primary id value for multiple relations?

```javascript
// primary key is id + autoincrement
var user = sql.insert('user', 'tbl_user');
user.set('name', 'Peter');

// Lock latest inserted identificator
sql.lock();
// is same as
// sql.put(sql.$$);

var address = sql.insert('tbl_user_address');
address.set('iduser', sql.$$); // adds latest primary id value
address.set('country', 'Slovakia');

var email = sql.insert('tbl_user_email');
email.set('iduser', sql.$$); // adds locked value
email.set('email', 'petersirka@gmail.com');
sql.unlock();

sql.exec();
```

### If not or If exists

```javascript
instance.ifnot('user', function(error, response, value) {
    // error === ErrorBuilder
    // It will be executed when the results `user` contains a negative value or array.length === 0
    // Is executed in order
});

instance.ifexists('user', function(error, response, value) {
    // error === ErrorBuilder
    // It will be executed when the results `user` contains a positive value or array.length > 0
    // Is executed in order
});
```

### Default values

- you can set default values
- values are bonded immediately (not in order)

```javascript
sql.default(function(response) {
    response.count = 0;
    response.user = {};
    response.user.id = 1;
});

// ...
// ...

sql.exec(function(err, response) {
    console.log(response);
});
```

### Modify results

- values are bonded in an order

```javascript
sql.select(...);
sql.insert(...);

sql.modify(function(response) {
    response.user = {};
    response.user.identity = 10;
});

// ...
// ...

// Calling:
// 1. select
// 2. insert
// 3. modify
// 4. other commands
sql.exec(function(err, response) {
    console.log(response);
});
```

### Preparing (dependencies)

- you can use multiple `sql.prepare()`

```javascript
var user = sql.update('user', 'tbl_user');
user.where('id', 20);
user.set('name', 'Peter');

var select = sql.select('address', 'tbl_address');
select.where('isremoved', false);
select.and();
select.where('city', 'Bratislava');
select.limit(1);

// IMPORTANT:
sql.prepare(function(error, response, resume) {
    // error === ErrorBuilder
    sql.builder('address').set('idaddress', response.address.id);
    resume();
});

var address = sql.update('address', 'tbl_user_address');
address.where('iduser', 20);

sql.exec();
```

### Validation

- you can use multiple `sql.validate()`

```plain
sql.validate(fn)
```

```javascript
var select = sql.select('address', 'tbl_address');
select.where('isremoved', false);
select.and();
select.where('city', 'Bratislava');
select.limit(1);

// IMPORTANT:
sql.validate(function(error, response, resume) {

    // error === ErrorBuilder

    if (!response.address) {
        error.push('Sorry, address not found');
        // cancel pending queries
        return resume(false);
    }

    sql.builder('user').set('idaddress', response.id);

    // continue
    resume();
});

var user = sql.update('user', 'tbl_user');
user.where('id', 20);
user.set('name', 'Peter');

sql.exec();
```

__Validation alternative (+v4.0.0)__

```javascript
// IMPORTANT:
sql.validate(function(error, response) {

    // error === ErrorBuilder

    if (!response.address) {
        error.push('Sorry, address not found');
        return false;
    }

    sql.builder('user').set('idaddress', response.id);
    return true;
});
```

---

```plain
sql.validate([result_name_for_validation], error_message, [reverse]);
```

- `result_name_for_validation` (String) a result to compare.
- `error_message` (String) an error message
- `reverse` (Boolean) a reverse comparison (false: result must exist (default), true: result must be empty)
__

If the function throw error then SqlAgent cancel all pending queris (perform Rollback if the agent is in transaction mode) and executes callback with error.

```javascript
var select = sql.select('address', 'tbl_address');
select.where('isremoved', false);
select.and();
select.where('city', 'Bratislava');
select.limit(1);

// IMPORTANT:
sql.validate('Sorry, address not found');

var user = sql.select('user', 'tbl_user');
user.where('id', 20);

sql.validate('Sorry, user not found');
sql.validate('Sorry, address not found for the current user', 'address');

sql.exec();
```

__Validation alternative (+v8.0.0)__

```javascript
sql.validate('products', n => n.length > 0, 'error-products');
sql.validate('detail', n => !n, 'error-detail');
```


## Global

### Stored procedures

```javascript
sql.query('myresult', 'exec myprocedure');

// with params
// sql.query('myresult', 'exec myprocedure $1', [3403]);

sql.exec(function(err, response) {
    console.log(response.myresult);
});
```

### Skipper

```javascript
sql.select('users', 'tbl_users');
sql.skip(); // skip orders
sql.select('orders', 'tbl_orders');

sql.bookmark(function(error, response) {
    // error === ErrorBuilder
    // skip logs
    sql.skip('logs');
});

sql.select('logs', 'tbl_logs');

sql.exec(function(err, response) {
    console.log(response); // --- response will be contain only { users: [] }
});
```

### Bookmarks

Bookmark is same as `sql.prepare()` function but without `resume` argument.

```javascript
sql.select('users', 'tbl_users');

sql.bookmark(function(error, response) {
    // error === ErrorBuilder
    console.log(response);
    response['custom'] = 'Peter';
});

sql.select('orders', 'tbl_orders');

sql.exec(function(err, response) {
    response.users;
    response.orders;
    response.custom; // === Peter
});
```

### Error handling

```javascript
sql.select('users', 'tbl_users');

sql.validate(function(error, response, resume) {

    // error === ErrorBuilder

    if (!response.users || respone.users.length === 0)
        error.push(new Error('This is error'));

    // total.js:
    // error.push('error-users-empty');

    resume();
});

sql.select('orders', 'tbl_orders');

// sql.validate([error message], [result name for validation])
sql.validate('error-orders-empty');
// is same as:
// sql.validate('error-orders-empty', 'orders');

sql.validate('error-users-empty', 'users');
```

### Escaping values

- doesn't work with MongoDB

```javascript
var escaped1 = Agent.escape(value);

// or ...

var sql = new Agent();
var escaped2 = sql.escape(value);
```

### Predefined queries

- doesn't work with MongoDB

```plain
Agent.query(name, query);
```

```javascript
Agent.query('users', 'SELECT * FROM tbl_users');
Agent.query('allorders', 'SELECT * FROM view_orders');

sql.query('users').where('id', '>', 20);
sql.query('orders', 'allorders').limit(5);

sql.exec(function(err, response) {
    console.log(response[0]); // users
    console.log(response.orders); // orders
});
```

### Waiting for specified values

- `+3.1.0`

```javascript
sql.when('users', function(error, response, value) {
    console.log(value);
});

sql.when('orders', function(error, response, value) {
    console.log(value);
});

sql.select('users', 'tbl_users');
sql.select('orders', 'tbl_orders');
sql.exec();
```

## Bonus

### How to get latest inserted ID?

- doesn't work with MongoDB

```javascript
sql.insert('user', 'tbl_user').set('name', 'Peter');

sql.bookmark(function() {
    console.log(sql.id);
});

sql.exec();
```

### Expected values? No problem

- __MongoDB__ supports expected values only in conditions.

```plain
sql.expected(name, index, property); // gets a specific value from the array
sql.expected(name, property);
```

```javascript
sql.select('user', 'tbl_user').where('id', 1).first();
sql.select('products', 'tbl_product').where('iduser', sql.expected('user', 'id'));

sql.exec();
```

### Measuring time

```javascript
sql.exec(function(err, response) {
    console.log(sql.time + ' ms');
    // or
    // console.log(this.time)
});
```

### Events

```javascript
sql.on('query', function(name, query, params){});
sql.on('data', function(name, response){});
sql.on('end', function(err, response, time){});
```

### Generators in total.js

```javascript
function *some_action() {
    var sql = DB();

    sql.select('users', 'tbl_user').make(function(select) {
        select.where('id', '>', 100);
        select.and();
        select.where('id', '<', 1000);
        select.limit(10);
    });

    sql.select('products', 'tbl_product').make(function(select) {
        select.where('price', '<>', 10);
        select.limit(10);
    });

    // get all results
    var results = yield sync(sql.$$exec())();
    console.log(results);

    // or get a specific result:
    var result = yield sync(sql.$$exec('users'))();
    console.log(result);
}
```

### Priority

Set a command priority, so the command will be processed next round.

```javascript
sql.select('... processed as second')
sql.select('... processed as first');
sql.priority(); // --> takes last item in queue and inserts it as first (sorts it immediately).
```


### Debug mode

Debug mode writes each query to console.

```javascript
sql.debug = true;
```

### We need to return into the callback only one value from the response object

```javascript
sql.exec(callback, 0); // --> returns first value from response (if isn't error)
sql.exec(callback, 'users'); // --> returns response.users (if is isn't error)

sql.exec(function(err, response) {
    if (err)
        throw err;
    console.log(response); // response will contain only orders
}, 'orders');
```

## SqlBuilder

- automatically adds `and` if is not added between e.g. 2x where

```javascript
// Creates SqlBuilder
var builder = sql.$;

builder.where('id', '<>', 20);
builder.set('isconfirmed', true);

// e.g.:
sql.update('users', 'tbl_users', builder);
sql.exec(function(err, response) {
    console.log(response.users);
})
```

---

#### builder.callback(fn)

```plain
builder.callback(function(err, response) {

});
```

`+v11.0.0` returns a value from DB


---

#### builder.set()

```plain
builder.set(name, value)
```

adds a value for update or insert

- `name` (String) column name
- `value` (Object) value

---

#### builder.raw()

```plain
builder.raw(name, value)
```

adds a raw value for update or insert without SQL encoding

- `name` (String) column name
- `value` (Object) value

---

```plain
builder.set(obj)
```
adds an object for update or insert value collection

```javascript
builder.set({ name: 'Peter', age: 30 });
// is same as
// builder.set('name', 'Peter');
// builder.set('age', 30);
```

---

#### builder.inc()

```plain
builder.set(name, [type], value)
```

adds a value for update or insert

- `name` (String) column name
- `type` (String) increment type (`+` (default), `-`, `*`, `/`)
- `value` (Number) value

```javascript
builder.inc('countupdate', 1);
builder.inc('countview', '+', 1);
builder.inc('credits', '-', 1);

// Short write
builder.inc('countupdate', '+1');
builder.inc('credits', '-1');
```

---

#### builder.rem()

```plain
builder.rem(name)
```
removes an value for inserting or updating.

```javascript
builder.set('name', 'Peter');
builder.rem('name');
```

---

#### builder.sort()

```plain
builder.sort(name, [desc])
builder.order(name, [desc])
```
adds sorting

- `name` (String) column name
- `desc` (Boolean), default: false

#### builder.random()

```plain
builder.random()
```

Reads random rows. __IMPORTANT__: MongoDB doesn't support this feature.

---

#### builder.skip()

```plain
builder.skip(value)
```
skips records

- `value` (Number or String), string is automatically converted into number

---

#### builder.take()

```plain
builder.take(value)
builder.limit(value)
```
takes records

- `value` (Number or String), string is automatically converted into number

---

#### builder.page()

```plain
builder.page(page, maxItemsPerPage)
```
sets automatically sql.skip() and sql.take()

- `page` (Number or String), string is automatically converted into number
- `maxItemsPerPage` (Number or String), string is automatically converted into number

---

#### builder.first()

```plain
builder.first()
```
sets sql.take(1)

---

#### builder.join()

- doesn't work with MongoDB

```plain
builder.join(name, on, [type])
```

adds a value for update or insert

- `name` (String) table name
- `on` (String) condition
- `type` (String) optional, inner type `inner`, `left` (default), `right`

```javascript
builder.join('address', 'address.id=user.idaddress');
```

---

#### builder.where()

```plain
builder.where(name, [operator], value)
builder.push(name, [operator], value)
```
add a condition after SQL WHERE

- `name` (String) column name
- `operator` (String), optional `>`, `<`, `<>`, `=` (default)
- `value` (Object)

---

#### builder.group()

- doesn't work with MongoDB

```plain
builder.group(name)
builder.group(name1, name2, name3); // +v2.9.1
```
creates a group by in SQL query

- `name` (String or String Array)

---

#### builder.having()

- doesn't work with MongoDB

```plain
builder.having(condition)
```
adds having in SQL query

- `condition` (String), e.g. `MAX(Id)>0`

---

#### builder.and()

```plain
builder.and()
```
adds AND to SQL query. __IMPORTANT__: In MongoDB has to be this operator used before all queries.

---

#### builder.or()

```plain
builder.or()
```
adds OR to SQL query. __IMPORTANT__: In MongoDB has to be this operator used before all queries.

---

#### builder.in()

```plain
builder.in(name, value)
```
adds IN to SQL query

- `name` (String), column name
- `value` (String, Number or String Array, Number Array)

---

#### builder.between()

```plain
builder.between(name, a, b)
```
adds between to SQL query

- `name` (String), column name
- `a` (Number)
- `b` (Number)

---

#### builder.overlaps()

```plain
builder.overlaps(valueA, valueB, columnA, columnB)
```
- __only for PostgreSQL__

adds overlaps to SQL query

- `valueA` (String, Number, Date)
- `valueB` (String, Number, Date)
- `columnA` (String), column A name
- `columnB` (String), column B name

---

#### builder.like()

```plain
builder.like(name, value, [where])
```
adds like command

- `name` (String) column name
- `value` (String) value to search
- `where` (String) optional, e.g. `beg`, `end`, `*` ==> %search (beg), search% (end), %search% (*)

---

#### builder.sql()

- doesn't work with MongoDB

```plain
builder.sql(query, [param1], [param2], [param..n])
```
adds a custom SQL to SQL query

- `query` (String)

```javascript
builder.sql('age=? AND name=?', 20, 'Peter');
```

#### builder.query()

- works with MongoDB

```plain
builder.query(fieldname, filter)
```
adds a custom QUERY to filter.

```javascript
builder.query('tags', { $size: 0 });
```

---

#### builder.scope()

```plain
builder.scope(fn);
```
adds a scope `()`

```javascript
builder.where('user', 'person');
builder.and();

// RDMBS:
builder.scope(function() {
    builder.where('type', 20);
    builder.or();
    builder.where('age', '<', 20);
});

// MongoDB:
builder.scope(function() {
    builder.or();
    builder.where('type', 20);
    builder.where('age', '<', 20);
});

// creates: user='person' AND (type=20 OR age<20)
```

#### builder.define()

```plain
builder.define(name, SQL_TYPE_LOWERCASE);
```
- __only for SQL SERVER__
- change the param type

```javascript
var insert = sql.insert('user', 'tbl_user');

insert.set('name', 'Peter Širka');
insert.define('name', 'varchar');
insert.set('credit', 340.34);
insert.define('credit', 'money');
sql.exec();
```

---

#### builder.schema()

- doesn't work with MongoDB

```plain
builder.schema()
```
sets current schema for `where`, `in`, `between`, `field`, `fields`, `like`

```javascript
builder.schema('b');
builder.fields('name', 'age'); // --> b."name", b."age"
builder.schema('a');
builder.fields('name', 'age'); // --> a."name", a."age"
builder.fields('!COUNT(id) as count') // --> a.COUNT()
```

#### builder.escape()

- doesn't work with MongoDB

```plain
builder.escape(string)
```
escapes value as prevention for SQL injection

#### builder.fields()

```plain
builder.fields()
```
sets fields for data selecting.

```javascript
builder.fields('name', 'age'); // "name", "age"
builder.fields('!COUNT(id)'); // Raw field: COUNT(id)
builder.fields('!COUNT(id) --> number'); // Raw field with casting: COUNT(id)::int (in PG), CAST(COUNT(id) as INT) (in SQL SERVER), etc.
```

#### builder.replace()

```plain
builder.replace(builder, [reference])
```
replaces current instance of SqlBuilder with new. The argument `reference` (default: `false`) when is `true` creates a reference to `builder` (it doesn't clone it). Better performance with lower memory.

- `builder` (SqlBuilder) Another instance of SqlBuilder.


---

#### builder.toString()

- doesn't work with MongoDB

```plain
builder.toString()
```
creates escaped SQL query (internal)

## Blob

### PostgreSQL

- all file operations are executed just-in-time (you don't need to call `sql.exec()`)
- all file operations aren't executed in queue

```javascript
// sql.writeStream(filestream, [buffersize](default: 16384), callback(err, loid))
sql.writeStream(Fs.createReadStream('/file.png'), function(err, loid) {
    // Now is the file inserted
    // Where is the file stored?

    // loid === NUMBER
    // SELECT * FROM pg_largeobject WHERE loid=loid
});

// sql.writeBuffer(buffer, callback(err, loid))
sql.writeBuffer(Buffer.from('Peter Širka', 'utf8'), function(err, loid) {
    // Now is the buffer inserted
    // Where is the buffer stored?

    // loid === NUMBER
    // SELECT * FROM pg_largeobject WHERE loid=loid
});

// sql.readStream(loid, [buffersize](default: 16384), callback(err, stream, size))
sql.readStream(loid, function(err, stream, size) {
    // stream is created
});
```

### MongoDB

- all file operations are executed immediately, there's no need to call sql.exec()

```javascript
// nosql.writeStream(id, stream, filename, [metadata], [options], callback)
nosql.writeStream(new ObjectID(), Fs.createReadStream('logo.png'), 'logo.png', function(err) {
    // Now is the stream inserted
});

// nosql.readStream(id, [options], callback(err, stream, metadata, size, filename))
nosql.readStream(id, function(err, stream, metadata, size, filename) {
    stream.pipe(Fs.createWriteStream('myfile.png'));
});

// get file info
nosql.select('fs.files').make(function(builder){
    // available fields - _id,filename,contentType,length,chunkSize,uploadDate,aliases,metadata,md5
    builder.fields('filename', 'metadata');
});

nosql.exec(function(err, results){
    console.log(results);
});
```

## Global events

__Global events__:

```javascript
ON('database', function() {
    // Database is ready
});
```

## Async/Await

`+v12.0.0` supports `sql.promise([name], [callback(response)])` for using of async/await.

- `sql.promise()` performs `sql.exec()`
- look to example below:

```javascript
var Agent = require('sqlagent/pg').connect('...');

async function data() {
    var b = new Agent();
    b.select('users', 'tbl_users');
    var users = await b.promise('users');
    console.log(users);
}

data();
```

## Contributors

| Contributor | Type | E-mail |
|-------------|------|--------|
| [Peter Širka](https://github.com/JozefGula) | author + support | <petersirka@gmail.com> |
| [Martin Smola](https://github.com/molda) | contributor + support | <smola.martin@gmail.com> |
| [Jay Kelkar](https://github.com/jkelkar) | contributor | <jkelkar@gmail.com> |
| [Aidan Dunn](https://github.com/Aidan-Chey) | contributor | <aidancheyd@gmail.com> |

## Contact

Do you have any questions? Contact us <https://www.totaljs.com/contact/>

[![Professional Support](https://www.totaljs.com/img/badge-support.svg)](https://www.totaljs.com/support/) [![Chat with contributors](https://www.totaljs.com/img/badge-chat.svg)](https://messenger.totaljs.com)

[license-image]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat
[license-url]: license.txt

[npm-url]: https://npmjs.org/package/sqlagent
[npm-version-image]: https://img.shields.io/npm/v/sqlagent.svg?style=flat
[npm-downloads-image]: https://img.shields.io/npm/dm/sqlagent.svg?style=flat
