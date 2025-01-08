use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;

use apache_avro::schema::RecordField;
use apache_avro::schema::RecordSchema;
use apache_avro::Reader;
use apache_avro::Schema;

use csv::Writer;

use serde::Serialize;
use serde::Serializer;

use serde::ser::Error;
use serde::ser::SerializeSeq;

use uuid::Uuid;

use apache_avro::types::Value;

pub struct Val<'a> {
    pub value: &'a Value,
}

pub const NOT_UUID: &str = "not an uuid";

impl Val<'_> {
    pub fn serialize_uuid<S>(u: &Uuid, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        u.serialize(ser)
    }

    pub fn not_uuid<S>() -> S::Error
    where
        S: Serializer,
    {
        S::Error::custom(NOT_UUID)
    }

    pub fn serialize_uuid_value<S>(v: &Value, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match v {
            Value::Fixed(16, fixed) => Uuid::from_slice(fixed)
                .map_err(|_| Self::not_uuid::<S>())
                .and_then(|u: Uuid| Self::serialize_uuid(&u, ser)),
            _ => Err(Self::not_uuid::<S>()),
        }
    }
}

impl Serialize for Val<'_> {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self.value {
            Value::Null => ser.serialize_none(),
            Value::Boolean(b) => ser.serialize_bool(*b),
            Value::Int(i) => ser.serialize_i32(*i),
            Value::Long(i) => ser.serialize_i64(*i),
            Value::Float(i) => ser.serialize_f32(*i),
            Value::Double(i) => ser.serialize_f64(*i),
            Value::String(i) => ser.serialize_str(i),
            Value::Fixed(_, _) => Self::serialize_uuid_value(self.value, ser),
            Value::Enum(_, s) => ser.serialize_str(s),
            Value::Union(_, u) => {
                let v: &Value = u;
                let val: Val = Val { value: v };
                val.serialize(ser)
            }
            Value::TimestampMicros(i) => {
                let f: f64 = (*i as f64) * 1e-6;
                ser.serialize_f64(f)
            }
            Value::Record(v) => {
                let sz: usize = v.len();
                let mut qser = ser.serialize_seq(Some(sz))?;
                for pair in v {
                    let (_, val) = pair;
                    let v: &Value = val;
                    let vl: Val = Val { value: v };
                    qser.serialize_element(&vl)?;
                }
                qser.end()
            }
            _ => Err(S::Error::custom(format!(
                "unsupported type: {:#?}",
                self.value
            ))),
        }
    }
}

pub fn values2writer<I, W>(values: I, mut writer: Writer<W>) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<Value, io::Error>>,
    W: Write,
{
    for res in values {
        let val: Value = res?;
        let v: Val = Val { value: &val };
        writer.serialize(v)?;
    }

    writer.flush()
}

pub struct RecordFields<'a> {
    fields: &'a [RecordField],
}

impl Serialize for RecordFields<'_> {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let sz: usize = self.fields.len();
        let mut qser = ser.serialize_seq(Some(sz))?;
        for field in self.fields {
            let name: &str = &field.name;
            qser.serialize_element(name)?;
        }
        qser.end()
    }
}

pub fn reader2writer<R, W>(rdr: R, wtr: W) -> Result<(), io::Error>
where
    R: Read,
    W: Write,
{
    let mut bw = BufWriter::new(wtr);
    let br = BufReader::new(rdr);
    let dec: Reader<_> = Reader::new(br).map_err(io::Error::other)?;
    {
        let mut cw: Writer<_> = Writer::from_writer(&mut bw);

        let s: &Schema = dec.writer_schema();
        let rs: &RecordSchema = match s {
            Schema::Record(rs) => Ok(rs),
            _ => Err(io::Error::other("invalid schema")),
        }?;
        let fields: &[RecordField] = &rs.fields;
        let rf = RecordFields { fields };
        cw.serialize(rf)?;
        let records = dec.map(|r| r.map_err(io::Error::other));
        values2writer(records, cw)?;
    }
    bw.flush()
}

pub fn stdin2stdout() -> Result<(), io::Error> {
    let i = io::stdin();
    let il = i.lock();
    let o = io::stdout();
    let ol = o.lock();
    reader2writer(il, ol)
}
