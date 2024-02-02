using { db } from '../db/db';

@rest
service TestService {
    entity Customer as select from db.Customer;
}