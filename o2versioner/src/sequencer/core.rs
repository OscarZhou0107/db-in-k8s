use crate::core::msql::MsqlBeginTx;
use crate::core::operation::Operation;
use crate::core::{TxTableVN, TxVN, VN};
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
pub struct State {
    vn_record: HashMap<String, TableVNRecord>,
}

impl State {
    pub fn new() -> Self {
        Self {
            vn_record: HashMap::new(),
        }
    }

    pub fn assign_vn(&mut self, msqlbegintx: MsqlBeginTx) -> TxVN {
        let (tx, tableops) = msqlbegintx.unwrap();

        TxVN {
            tx,
            txtablevns: tableops
                .into_iter()
                .map(|tableop| TxTableVN {
                    table: tableop.table.clone(),
                    vn: self.vn_record.entry(tableop.table).or_default().assign(&tableop.op),
                    op: tableop.op,
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
    use crate::core::msql::MsqlBeginTx;
    use crate::core::operation::*;
    use crate::core::{TxTableVN, TxVN};
    use std::iter::FromIterator;

    #[test]
    fn test_assign_vn() {
        let mut state = State::new();

        //                   a     b     c
        // next_for_read     0     0     0
        // next_for_write    0     0     0
        assert_eq!(
            state.assign_vn(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("a", Operation::W),
                TableOp::new("b", Operation::W),
                TableOp::new("c", Operation::R)
            ]))),
            TxVN {
                tx: None,
                txtablevns: vec![
                    TxTableVN {
                        table: String::from("a"),
                        vn: 0,
                        op: Operation::W,
                    },
                    TxTableVN {
                        table: String::from("b"),
                        vn: 0,
                        op: Operation::W,
                    },
                    TxTableVN {
                        table: String::from("c"),
                        vn: 0,
                        op: Operation::R,
                    }
                ]
            }
        );

        //                   a     b     c
        // next_for_read     1     1     0
        // next_for_write    1     1     1
        assert_eq!(
            state.assign_vn(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("b", Operation::W),
                TableOp::new("c", Operation::R)
            ]))),
            TxVN {
                tx: None,
                txtablevns: vec![
                    TxTableVN {
                        table: String::from("b"),
                        vn: 1,
                        op: Operation::W,
                    },
                    TxTableVN {
                        table: String::from("c"),
                        vn: 0,
                        op: Operation::R,
                    }
                ]
            }
        );

        //                   a     b     c
        // next_for_read     1     2     0
        // next_for_write    1     2     2
        assert_eq!(
            state.assign_vn(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("b", Operation::R),
                TableOp::new("c", Operation::W)
            ]))),
            TxVN {
                tx: None,
                txtablevns: vec![
                    TxTableVN {
                        table: String::from("b"),
                        vn: 2,
                        op: Operation::R,
                    },
                    TxTableVN {
                        table: String::from("c"),
                        vn: 2,
                        op: Operation::W,
                    }
                ]
            }
        );

        //                   a     b     c
        // next_for_read     1     2     3
        // next_for_write    1     3     3
        assert_eq!(
            state.assign_vn(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("a", Operation::R),
                TableOp::new("b", Operation::R),
                TableOp::new("c", Operation::W)
            ],))),
            TxVN {
                tx: None,
                txtablevns: vec![
                    TxTableVN {
                        table: String::from("a"),
                        vn: 1,
                        op: Operation::R,
                    },
                    TxTableVN {
                        table: String::from("b"),
                        vn: 2,
                        op: Operation::R,
                    },
                    TxTableVN {
                        table: String::from("c"),
                        vn: 3,
                        op: Operation::W,
                    }
                ]
            }
        );

        //                   a     b     c
        // next_for_read     1     2     4
        // next_for_write    2     4     4
    }
}
