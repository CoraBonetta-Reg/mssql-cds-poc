using { db } from '../db/db';

service TestService {
    entity Customer as projection on db.Customer;
}