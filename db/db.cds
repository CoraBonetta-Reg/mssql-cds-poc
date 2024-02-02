using {
    cuid,
    managed
} from '@sap/cds/common';

namespace db;

entity Customer : cuid {
   CustomerName : String; 
}