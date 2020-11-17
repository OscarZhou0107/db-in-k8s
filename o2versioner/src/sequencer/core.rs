use crate::common::sql::{Operation, TxTable};
use crate::common::version_number::{TableVN, TxVN, VN};
use std::collections::HashMap;

/// Version number info for a single table
#[derive(Default, Debug, Eq, PartialEq)]
struct TableVNRecord {
    next_for_read: VN,
    next_for_write: VN,
}

impl TableVNRecord {
    fn assign_read(&mut self) -> VN {
        let vn_read = self.next_for_read;

        self.next_for_write += 1;

        return vn_read;
    }

    fn assign_write(&mut self) -> VN {
        let vn_write = self.next_for_write;

        self.next_for_write += 1;
        self.next_for_read = self.next_for_write;

        return vn_write;
    }

    fn assign(&mut self, op: &Operation) -> VN {
        match op {
            Operation::R => self.assign_read(),
            Operation::W => self.assign_write(),
        }
    }
}

/// Sequencer state
#[allow(dead_code)]
#[derive(Default)]
struct State {
    vn_record: HashMap<String, TableVNRecord>,
}

impl State {
    #[allow(dead_code)]
    fn assign_vn(&mut self, tx_table: TxTable) -> TxVN {
        TxVN {
            table_vns: tx_table
                .table_ops
                .into_iter()
                .map(|table_op| TableVN {
                    table: table_op.table.clone(),
                    vn: self
                        .vn_record
                        .entry(table_op.table)
                        .or_default()
                        .assign(&table_op.op),
                    op: table_op.op,
                })
                .collect(),
        }
    }
}

/// Unit test for `TableVNRecord`
#[cfg(test)]
mod tests_table_vn_record {
    use super::TableVNRecord;

    #[test]
    fn test_assign_read() {
        let mut vnr: TableVNRecord = Default::default();

        assert_eq!(vnr.assign_read(), 0);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 0,
                next_for_write: 1
            }
        );

        assert_eq!(vnr.assign_read(), 0);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 0,
                next_for_write: 2
            }
        );
    }

    #[test]
    fn test_assign_write() {
        let mut vnr: TableVNRecord = Default::default();

        assert_eq!(vnr.assign_write(), 0);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 1,
                next_for_write: 1
            }
        );

        assert_eq!(vnr.assign_write(), 1);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 2,
                next_for_write: 2
            }
        );
    }

    #[test]
    fn test_assign_read_and_write() {
        // operation         w w r w r r r w
        // next_for_read    0 1 2 2 4 4 4 4 8
        // next_for_write   0 1 2 3 4 5 6 7 8
        // version assigned  0 1 2 3 4 4 4 7
        let mut vnr: TableVNRecord = Default::default();

        assert_eq!(vnr.assign_write(), 0);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 1,
                next_for_write: 1
            }
        );

        assert_eq!(vnr.assign_write(), 1);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 2,
                next_for_write: 2
            }
        );

        assert_eq!(vnr.assign_read(), 2);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 2,
                next_for_write: 3
            }
        );

        assert_eq!(vnr.assign_write(), 3);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 4,
                next_for_write: 4
            }
        );

        assert_eq!(vnr.assign_read(), 4);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 4,
                next_for_write: 5
            }
        );

        assert_eq!(vnr.assign_read(), 4);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 4,
                next_for_write: 6
            }
        );

        assert_eq!(vnr.assign_read(), 4);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 4,
                next_for_write: 7
            }
        );

        assert_eq!(vnr.assign_write(), 7);
        assert_eq!(
            vnr,
            TableVNRecord {
                next_for_read: 8,
                next_for_write: 8
            }
        );
    }
}

/// Unit test for `State`
#[cfg(test)]
mod tests_state {
    use super::State;

    #[test]
    fn test_assign_vn() {
        let mut state: State = Default::default();
        let _txvn = state.assign_vn(Default::default());
    }
}
