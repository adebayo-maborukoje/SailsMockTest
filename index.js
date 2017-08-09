var Promise = require('bluebird');

var _ = require('lodash');


const create_special_where = (operators) => {
  const predicates = _.entries(operators).map(([operator, compare_value]) => {
    if (operator === '>') {
      return (value) => value > compare_value;
    } else if (operator === '<' ) {
      return (value) => value < compare_value;
    } else if (operator === 'like') {
      const regexp = new RegExp(compare_value.replace(/%/g, '.*'));
      return (value) => regexp.test(value);
    } else {
      throw new Error(`Unknown operator '${operator}'`);
    }
  });

  return (value) => predicates.every(pred => pred(value));
}

const create_where = (criteria = {}) => {
  if (criteria == null) {
    throw new Error(`Criteria to create_where can't be null`);
  }
  
  if (typeof criteria === 'number') {
    return create_where({ id: criteria });
  }
  
  if (criteria.where) {
    return create_where(criteria.where);
  }

  const predicates = _.entries(criteria).map(([key, expected]) => {
    if (expected instanceof Date) {
      throw new Error(`TODO Date?!`);
    }
    if (key === 'and') {
      const sub_predicates = expected.map(sub_criteria => {
        const sub_predicate = create_where(sub_criteria);
        return (obj) => sub_criteria(obj);
      });
      return (obj) => sub_predicates.every(pred => pred(obj));
    } else if (key === 'or') {
      const sub_predicates = expected.map(sub_criteria => {
        const sub_predicate = create_where(sub_criteria);
        return (obj) => sub_predicate(obj);
      });
      return (obj) => sub_predicates.some(pred => pred(obj));
    } else if (_.isArray(expected)) {
      const sub_predicates = expected.map(exp => {
        const items = {
          [key]: exp
        }
        const sub_predicate = create_where(items);
        return (obj) => sub_predicate(obj)
      });
      return (obj) => sub_predicates.some(pred => pred(obj))
    }
    else if (typeof expected === 'object') {
      const sub_predicate = create_special_where(expected);
      return (obj) => sub_predicate(obj[key]);
    } 
    
    else {
      return (obj) => {
        return obj[key] === expected;
      }
    }
  });

  return (obj) => predicates.every(pred => pred(obj));
}
const invariant = (predicate, message) => {
  if (!predicate) {
    console.warn(`INVARIANT: ${message}`);
    throw new Error(message);
  }
}

const wrap_item_with_save = (item, model) => {
  const prototype = {
    save(possible_cb) {
      return model.update(item.id, item).then(changed_items => {
        if (typeof possible_cb === 'function') {
          possible_cb(null, changed_items[0]);
        }
        return changed_items;
      }).catch(err => {
        console.log(`err:`, err);
        if (typeof possible_cb === 'function') {
          possible_cb(err);
        }
      })
    }
  }
  if (item ) Object.setPrototypeOf(item, prototype);
  return item;
}


class Query {
  constructor(items, model, collections) {

    invariant(typeof items.then === 'function', `First argument to Query should be a promise, got ${items}`);
    this.items = items;
    this.model = model;
    this.collections = collections;
  }

  populate_collection(on_column, { collection, via }, { where }) {
    const new_items_promise = this.items.then(item_or_items => {
      invariant(item_or_items != null, `item_or_items is null in .populate { collection: ${collection}, via: ${via} }.`);
      const join_mock = this.collections.get_by_name(collection);
      const join_node = join_mock.model._attributes[via];

      const is_single_item = !Array.isArray(item_or_items);
      const items = is_single_item ? [item_or_items] : item_or_items;

      if (!join_node.collection && join_node.model) {
        const join_table = this.collections.get_by_name(join_node.model);
        return Promise.map(items, (item) => {
          return join_mock.find(Object.assign({}, {
             [via]: item.id,
          }, where))
          .then(joined_items => {
            return Object.assign({}, item, {
              [on_column]: joined_items,
            })
          })
        })
        .then(new_items => {
          return is_single_item ? new_items[0] : new_items;
        })
      } else {
        const us_to_join = `${collection}_${via}`.toLowerCase();
        const join_to_them = `${join_node.collection}_${join_node.via}`.toLowerCase();
        const intermediate_table_name = [us_to_join, join_to_them].sort().join(`_`);
        const intermediate_table = this.collections.get_by_name(intermediate_table_name);

        return Promise.map(items, (item) => {
          return intermediate_table.find({
            [us_to_join]: item.id,
          })
          .then((intermediate_items) => {
            // For every of these items, find the correct one in the join table
            return Promise.map(intermediate_items, item => {
              return join_mock.findOne({ id: item[join_to_them] });
            });
          })
          .then(join_items => {
            const where_clause = create_where(where);
            const filter_item  = where ? join_items.filter(item => where_clause(item)) : join_items;
            return Object.assign({}, item, {
              [on_column]: filter_item,
            })
          })
        })
        .then(fully_populated_items => {
          return is_single_item ? fully_populated_items[0] : fully_populated_items;
        });
      }
    });
    return new Query(new_items_promise, this.model, this.collections);
  }

  populate(on_column, options) {
    options = options || {};
    let where = options.where || {};

    const new_items_promise = this.items.then(item_or_items => {
      const attr = this.model.attributes[on_column];

      if (!attr.model && !attr.collection)  
        throw new Error('Attribute is not a model')
      if (attr.collection && attr.via) {
        try {
          return this.populate_collection(on_column, attr, { where: where });
        } catch (e) {
          console.log('e:', e);
        }
      }

      invariant(!options.where, `Where not yet supported for simple joins`);

      const join_model = this.collections.get_by_name(attr.model);

      if (Array.isArray(item_or_items)) {
        return Promise.map(item_or_items, item => {
          return join_model.findOne({ id: item[on_column] }).then(result => {
            return Object.assign({}, item, {
              [on_column]: result,
            });
          })
        })
      } else if (item_or_items) {
        let item = item_or_items;
        return join_model.findOne({ id: item[on_column] }).then(result => {
            return Object.assign({}, item, {
              [on_column]: result,
            });
          })
      } else {
        return this;
      }
    });
    return new Query(new_items_promise, this.model, this.collections);
  }

  then(...args) {
    return Promise.resolve(this.items).then(...args);
  }

  catch(fn) {
    return this.items.catch(fn);
  }
}


class MockTable {
  constructor(model, items, collections) {
    this.items = items;
    this.model = model;
    this.collections = collections;
    this.changes = []
  }

  readonly() {
    return this;
  }

  find(criteria) {
    const where_clause = create_where(criteria);
    const items = this.items.filter(item => where_clause(item));
    const wrapped_items = items.map(item => {
      return wrap_item_with_save(item, this);
    });
    return new Query(Promise.resolve(wrapped_items), this.model, this.collections);
  }

  findOne(criteria) {
    let where_clause = create_where(criteria);
    const items = this.items.filter(item => where_clause(item));
    const wrapped_items = items.map(item => {
      return wrap_item_with_save(item, this);
    });
    return new Query(Promise.resolve(wrapped_items[0]), this.model, this.collections);
  }

  findOrCreate(criteria) {
    const item = this.findOne(criteria);
    if (!item) {
      this.create(criteria);
    }
    return this.findOne(criteria)
  }

  update(criteria, update, query) {
    const where_clause = create_where(criteria);
    const updated_things = this.items.filter(where_clause);
    const updated_ids = updated_things.map(x => x.id);
    this.items = this.items.map(item => {
      if (where_clause(item)){
        var newObject = Object.assign({}, item, update);
        return newObject;
      } else {
        return item;
      }
    });
    this.changes.push({
      type: 'update',
      createdAt: new Date(),
      updated_ids: updated_ids,
      query: query || 2,
      update: update,
    });
    return Promise.resolve(updated_things);
  }

  create(item) {
    const with_id = Object.assign({}, item, {
      id: this.items.length + 1,
    });

    this.changes.push({
      type: 'create',
      createdAt: new Date(),
      item: with_id,
    });
    this.items.push(with_id);
    const wrapped_item = wrap_item_with_save(with_id, this);
    return new Query(Promise.resolve(wrapped_item), this.model, this.collections);
  }

  destroy(criteria) {
    const where_clause = create_where(criteria);

    this.items = this.items.filter(item => !where_clause(item));

    this.changes.push({
      type: 'destroy',
      createdAt: new Date(),
    });
    return Promise.resolve();

  }
};

class MockModels {
  constructor() {
    this.collections = {};
  }

  get_all_changes() {
    return _.flatten(_.entries(this.collections).map(([collectionName, collection]) => {
      return collection.mock.changes.map(change => {
        return Object.assign({}, change, {
          collectionName: collection.name,
        })
      });
    }))
    .sort((a, b) => a.createdAt - b.createdAt);
  }

  get_by_name(name) {
    if (this.collections[name.toLowerCase()]) {
      return this.collections[name.toLowerCase()].mock;
    }
    else {
      throw new Error(`No collection found called '${name}`);
    }
  }
  get_collection_by_name (name) {
    if (this.collections[name.toLowerCase()]) {
      return this.collections[name.toLowerCase()].mock;
    }
    else {
      throw new Error(`No collection found called '${name}`);
    }
  }
  mock_table(name, items) {
    let mock = new MockTable(global[name], items, this);
    if (global[name]) {
      global[name].name = name;
    } else {
      console.log('Not a table?', name, global[name]);
    }
    this.collections[name.toLowerCase()] = {
      original: global[name],
      name: name,
      mock: mock,
    };
  }

  apply() {
    Object.values(this.collections).forEach(col => {
      global[col.name] = col.mock;
    })
  }

  de_apply() {
    Object.values(this.collections).forEach(col => {
      global[col.name] = col.original;
    })
  }
} 
module.exports.MockModels = MockModels;
