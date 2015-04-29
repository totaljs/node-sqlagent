# Very helpful ORM for node.js

`npm install sqlagent`

- Currently supports __PostgreSQL__ only
- MySQL is in progress
- Simple and powerful

__IMPORTANT__:

- code is executed as it is added
- rollback is executed automatically when is transaction enable

## Initialization

### Basic initialization

```javascript
var Agent = require('sqlagent/pg');
var sql = new Agent('connetion-string-to-postgresql');
```

### Initialization for total.js

```javascript
// Below code rewrites total.js database prototype
require('sqlagent/pg').init('connetion-string-to-postgresql', [debug]);

// var sql = DATABASE([ErrorBuilder]);
var sql = DATABASE();
// sql === SqlAgent
```

## Usage

### Select

__instance.select([name], table, [columns])__:

- `name` (String) is identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- `colums` (String, Array or Object (keys will be as columns))
- __returns__ SqlBuilder

```javascript
var users = sql.select('users', 'tbl_user', '*');
users.where('id', '>', 5);
users.page(10, 10);

var products = sql.select('products', 'tbl_products', ['id', 'name']);
products.between('price', 30, 50);
products.and();
products.where('isremoved', false);
products.limit(20);

var admin = sql.select('admin', 'tbl_admin', { id: true, name: 1, age: null });
// SELECT id, name, age
admin.where('hash', 'petersirka');
admin.first();

sql.exec(function(err, response) {
    console.log(response.users);
    console.log(response.products);
    console.log(response.admin);
});
```

### Insert

__instance.insert([name], table, [value], [primaryId])__:

- `name` (String) is identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- `value` (Object) optional (value can be SqlBuilder)
- __returns__ if value is undefined then __SqlBuilder__ otherwise __SqlAgent__

```javascript
sql.insert('user', 'tbl_user', { name: 'Peter', age: 30 });

var insert = sql.insert('log', 'tbl_logs');
insert.set('message', 'Some log message.');
insert.set('created', new Date());

sql.exec(function(err, response) {
    console.log(response.user);
    console.log(response.log);
});
```

### Update

__instance.update([name], table, [value])__:

- `name` (String) is identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- `value` (Object) optional (value can be SqlBuilder)
- __returns__ if value is undefined then __SqlBuilder__ otherwise __SqlAgent__

```javascript
var update1 = sql.update('user1', 'tbl_user', { name: 'Peter', age: 30 });
update1.where('id', 1);

// is same as

var update2 = sql.update('user2', 'tbl_user');
update2.where('id', 1);
update2.set('name', 'Peter');
update2.set('age', 30);

sql.exec(function(err, response) {
    console.log(response.user1);
    console.log(response.user2);
});
```

### Delete

__instance.delete([name], table)__:
__instance.remove([name], table)__:

- `name` (String) is identificator for results, optional (default: internal indexer)
- `table` (String) table name, the library automatically creates SQL query
- __returns__ SqlBuilder

```javascript
var remove = sql.remove('user', 'tbl_user');
remove.where('id', 1);

sql.exec(function(err, response) {
    console.log(response.user);
});
```

### Aggregation

__instance.count([name], table)__:

- __returns__ SqlBuilder

```javascript
var count = sql.count('users', 'tbl_user');
count.between('age', 20, 40);

sql.exec(function(err, response) {
    console.log(response.users);
});
```

__instance.max([name], table, column)__:
__instance.min([name], table, column)__:
__instance.avg([name], table, column)__:

- __returns__ SqlBuilder

```javascript
var max = sql.max('users', 'tbl_user', 'age');
max.where('isremoved', false);

sql.exec(function(err, response) {
    console.log(response.users);
});
```

### Transactions

- rollback is executed automatically

```javascript
sql.begin();
sql.insert('tbl_user', { name: 'Peter' });
sql.commit();
```

## Special cases

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

### Preparing (dependencies)

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
    sql.builder('address').set('idaddress', response.address.id);
    resume();
});

var address = sql.update('address', 'tbl_user_address');
address.where('iduser', 20);

sql.exec();
```

### Validation

```javascript
var select = sql.select('address', 'tbl_address');
select.where('isremoved', false);
select.and();
select.where('city', 'Bratislava');
select.limit(1);

// IMPORTANT:
sql.validate(function(error, response, resume) {
    
    if (!response.address) {
        error.push('Sorry, address not found');
        // cancel pending queries
        return resume(false);
    }
    
    sql.builder('user').set('idaddress', response.id);
    resume();
});

var user = sql.update('user', 'tbl_user');
user.where('id', 20);
user.set('name', 'Peter');

sql.exec();
```

## SqlBuilder

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

__builder.set(name, value)__:
adds a value for update or insert

- `name` (String) a column name
- `value` (Object) a value

__builder.set(obj)__:
adds a value for update or insert

__builder.sort(name, [desc])__:
__builder.order(name, [desc])__:
adds sorting

- `name` (String) a column name
- `desc` (Boolean), default: false

__builder.skip(value)__:
skips records

__builder.take(value)__:
__builder.limit(value)__:
takes records

__builder.page(page, maxItemsPerPage)__:
sets automatically sql.skip() and sql.take()

__builder.first()__:
sets sql.take(1)

__builder.where(name, [operator], value)__:
__builder.push(name, [operator], value)__:
sets a where condition.

- `name` (String) a column name
- `operator` (String), optional `>`, `<`, `<>`, `=` (default)
- `value` (Object)

__builder.group(names)__:
creates a group by in SQL query

- `name` (String or String Array)

__builder.having(condition)__:
adds having in SQL query

- `condition` (String), e.g. `MAX(Id)>0`

__builder.and()__:
adds AND to SQL query

__builder.or()__:
adds AND to SQL query

__builder.in(name, value)__:
adds IN to SQL query

- `name` (String), a column name
- `value` (String, Number or String Array, Number Array)

__builder.between(name, a, b)__:
adds between to SQL query

- `name` (String), a column name
- `a` (Number)
- `b` (Number)

__builder.sql(query)__:
adds a custom SQL to SQL query

- `query` (String)

__builder.toString()__:
creates escaped SQL query (internal)









