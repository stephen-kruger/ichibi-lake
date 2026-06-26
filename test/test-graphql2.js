import express from 'express';
import { createHandler } from 'graphql-http/lib/use/express';
import { buildSchema, GraphQLScalarType } from 'graphql';

const schema = buildSchema(`
  scalar JSON
  type Query {
    query(sql: String!): JSON
  }
`);

const root = {
  query: ({ sql }) => {
    return [{ id: 1, val: "test" }];
  }
};

const app = express();
app.all('/graphql', createHandler({ schema, rootValue: root }));
app.listen(3334, () => console.log('Listening'));
