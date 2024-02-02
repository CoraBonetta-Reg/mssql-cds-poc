using {db} from '../db/db';

service TestService {
    entity Customer as select from db.Customer;
}
