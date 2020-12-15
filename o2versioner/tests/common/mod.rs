use o2versioner::comm::scheduler_api::*;
use o2versioner::core::*;

pub fn sql_transaction_samples() -> Vec<Vec<Message>> {
    vec![
        vec![
            Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 WRITE w1 w2")),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some("r0"))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some("w1"),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w2 set name=\"ray\" where id = 22;",
                "write w2",
                Some("w2"),
            )),
            Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
        ],
        vec![
            Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 r1 WRITE w1 w2 w3")),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some("r0"))),
            Message::RequestMsqlText(MsqlText::query(
                "update w2 set name=\"ray\" where id = 20;",
                "write w2",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r1;", "read r1", Some("r1"))),
            Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2", Some("w2"))),
            Message::RequestMsqlText(MsqlText::query(
                "update w3 set name=\"ray\" where id = 22;",
                "write w3",
                Some("w3"),
            )),
            Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
        ],
        vec![
            Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ w2 WRITE w1 r0 r1")),
            Message::RequestMsqlText(MsqlText::query("select * from w1;", "read w1", Some("w1"))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query(
                "update w1 set name=\"ray\" where id = 20;",
                "write w1",
                Some(""),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0", Some(""))),
            Message::RequestMsqlText(MsqlText::query(
                "update r0 set name=\"ray\" where id = 20;",
                "write r0",
                Some("r0"),
            )),
            Message::RequestMsqlText(MsqlText::query("select * from r1;", "read r1", Some(""))),
            Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2", Some("w2"))),
            Message::RequestMsqlText(MsqlText::query(
                "update r1 set name=\"ray\" where id = 22;",
                "write r1",
                Some("r1"),
            )),
            Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
        ],
        // #3 Single read
        vec![Message::RequestMsqlText(MsqlText::query(
            "select * from w1;",
            "read w1",
            Some(""),
        ))],
        // #4 Single write
        vec![Message::RequestMsqlText(MsqlText::query(
            "update w3 set name=\"ray\" where id = 22;",
            "write w3",
            Some(""),
        ))],
        // #5 Single read with early release
        vec![Message::RequestMsqlText(MsqlText::query(
            "select * from w1;",
            "read w1",
            Some("w1"),
        ))],
        // #6 Single write with early release
        vec![Message::RequestMsqlText(MsqlText::query(
            "update w3 set name=\"ray\" where id = 22;",
            "write w3",
            Some("w3"),
        ))],
    ]
}
