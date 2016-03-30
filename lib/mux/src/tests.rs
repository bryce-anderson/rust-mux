use super::*;

use std::io;

fn new_write() -> io::Cursor<Vec<u8>> {
    io::Cursor::new(Vec::new())
}

fn roundtrip_frame(msg: MessageFrame) {
    let msg = Message {
        tag: Tag::new(true, 2),
        frame: msg,
    };

    let mut w = new_write();
    let _ = encode_message(&mut w, &msg).unwrap();
    let w = w.into_inner();
    let decoded = read_message(&mut io::Cursor::new(w)).unwrap();

    assert_eq!(&msg, &decoded);
}

#[test]
fn roundtrip_treq() {
    roundtrip_frame(MessageFrame::Treq(Treq {
        headers: vec![(1, vec![4, 5, 6])],
        body: vec![1, 2, 3],
    }));

    roundtrip_frame(MessageFrame::Treq(Treq {
        headers: Vec::new(),
        body: Vec::new(),
    }));
}

#[test]
fn roundtrip_rreq() {
    roundtrip_frame(MessageFrame::Rreq(Rmsg::Ok(vec![1, 2, 3])));
    roundtrip_frame(MessageFrame::Rreq(Rmsg::Nack("Boo".to_owned())));
    roundtrip_frame(MessageFrame::Rreq(Rmsg::Error("Boo".to_owned())));
}

#[test]
fn roundtrip_tdispatch() {

    roundtrip_frame(MessageFrame::Tdispatch(Tdispatch {
        contexts: vec![(vec![1, 2, 3], vec![4, 5, 6])],
        dest: "foo".to_string(),
        dtab: Dtab::from(vec![("foo".to_string(), "bar".to_string())]),
        body: vec![1, 2, 3],
    }));

    roundtrip_frame(MessageFrame::Tdispatch(Tdispatch {
        contexts: Vec::new(),
        dest: "foo".to_string(),
        dtab: Dtab::new(),
        body: Vec::new(),
    }));

}

#[test]
fn roundtrip_rdispatch() {
    roundtrip_frame(MessageFrame::Rdispatch(Rdispatch {
        contexts: vec![(vec![1, 2, 3], vec![4, 5, 6])],
        msg: Rmsg::Ok(vec![1, 2, 3]),
    }));

    roundtrip_frame(MessageFrame::Rdispatch(Rdispatch {
        contexts: Vec::new(),
        msg: Rmsg::Nack("Boo".to_owned()),
    }));

    roundtrip_frame(MessageFrame::Rdispatch(Rdispatch {
        contexts: Vec::new(),
        msg: Rmsg::Error("Boo".to_owned()),
    }));
}

#[test]
fn roundtrip_tinit() {
    roundtrip_frame(MessageFrame::Tinit(Init {
        version: 12,
        headers: vec![(vec![1, 2, 3], vec![4, 5, 6, 7])],
    }));

    roundtrip_frame(MessageFrame::Tinit(Init {
        version: 1,
        headers: vec![(vec![43, 127], vec![])],
    }));
}

#[test]
fn roundtrip_rinit() {
    roundtrip_frame(MessageFrame::Rinit(Init {
        version: 12,
        headers: vec![(vec![1, 2, 3], vec![4, 5, 6, 7])],
    }));

    roundtrip_frame(MessageFrame::Rinit(Init {
        version: 1,
        headers: vec![(vec![43, 127], vec![])],
    }));
}

#[test]
fn roundtrip_tdrain() {
    roundtrip_frame(MessageFrame::Tdrain);
}

#[test]
fn roundtrip_rdrain() {
    roundtrip_frame(MessageFrame::Rdrain);
}

#[test]
fn roundtrip_tping() {
    roundtrip_frame(MessageFrame::Tping);
}

#[test]
fn roundtrip_rping() {
    roundtrip_frame(MessageFrame::Rping);
}

#[test]
fn roundtrip_rerr() {
    roundtrip_frame(MessageFrame::Rerr("Foo!".to_owned()));
}

#[test]
fn roundtrip_tlease() {
    use std::time::Duration;

    // note that this will fail for precision below 1 ms
    roundtrip_frame(MessageFrame::Tlease(Duration::new(123, 1_000_000)));
}

#[test]
fn roundtrip_dtab() {
    fn roundtrip_frame(table: &Dtab) {
        let mut w = new_write();
        let _ = frames::encode_dtab(&mut w, table).unwrap();
        let mut w = io::Cursor::new(w.into_inner());
        let decoded = frames::decode_dtab(&mut w).unwrap();

        assert_eq!(table, &decoded);
    }

    let mut tab = Dtab::new();

    roundtrip_frame(&tab);
    tab.add_entry("a".to_string(), "b".to_string());
    roundtrip_frame(&tab);
    tab.add_entry("c".to_string(), "d".to_string());
    roundtrip_frame(&tab);
}


#[test]
fn roundtrip_context() {
    fn roundtrip_frame(ctx: &Contexts) {
        let mut w = new_write();
        let _ = frames::encode_contexts(&mut w, ctx).unwrap();
        let mut w = io::Cursor::new(w.into_inner());
        let decoded = frames::decode_contexts(&mut w).unwrap();

        assert_eq!(ctx, &decoded);


    }

    roundtrip_frame(&Vec::new());
    roundtrip_frame(&vec![(vec![1, 2, 3], vec![4, 5, 6])]);
}


#[test]
fn roundtrip_tag() {
    fn roundtrip_frame(tag: &Tag) {
        let mut w = new_write();
        let _ = Tag::encode_tag(&mut w, &tag).unwrap();
        let mut w = io::Cursor::new(w.into_inner());
        let decoded = Tag::decode_tag(&mut w).unwrap();

        assert_eq!(tag, &decoded);
    }

    roundtrip_frame(&Tag {
        end: false,
        id: 1,
    });

    roundtrip_frame(&Tag { end: true, id: 1 });

    roundtrip_frame(&Tag {
        end: false,
        id: 0x0fffff,
    });
}
