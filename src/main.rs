pub trait Numeric {}
impl Numeric for f64 {}
impl Numeric for f32 {}
impl Numeric for i64 {}
impl Numeric for i32 {}
impl Numeric for i16 {}
impl Numeric for i8 {}
impl Numeric for isize {}
impl Numeric for u64 {}
impl Numeric for u32 {}
impl Numeric for u16 {}
impl Numeric for u8 {}
impl Numeric for usize {}

fn quote(x: &str) -> String {
    return format!("'{}'", &x);
}

fn snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_char: char = '_';

    for ch in s.chars() {
        if ch.is_uppercase() & prev_char.is_lowercase() {
            result.push('_');
        }
        for lower in ch.to_lowercase() {
            result.push(lower);
        }

        prev_char = ch;
    }

    result
}

pub enum SQLComp {
    EQ,
    NEQ,
    GT,
    LT,
    GEQ,
    LEQ,
}

pub trait ToSql {
    fn to_sql(&self) -> String;

    fn op_eq(&self) -> &str {
        "="
    }
    fn op_neq(&self) -> &str {
        "<>"
    }

    fn op_gt(&self) -> &str {
        ">"
    }

    fn op_lt(&self) -> &str {
        "<"
    }

    fn op_geq(&self) -> &str {
        ">="
    }

    fn op_leq(&self) -> &str {
        "<="
    }

    fn compare(&self, cmp: &SQLComp) -> String {
        let op = match cmp {
            SQLComp::EQ => self.op_eq(),
            SQLComp::NEQ => self.op_neq(),
            SQLComp::GT => self.op_gt(),
            SQLComp::LT => self.op_lt(),
            SQLComp::GEQ => self.op_geq(),
            SQLComp::LEQ => self.op_leq(),
        };

        format!("{} {}", op, self.to_sql())
    }
}

impl ToSql for &str {
    fn to_sql(&self) -> String {
        quote(self)
    }
}

impl ToSql for String {
    fn to_sql(&self) -> String {
        quote(self)
    }
}

/*
==============================================================
  Ideally we would want to implement this but Rust complains
--------------------------------------------------------------

impl<T: AsRef<str>> ToSql for T {
    fn to_sql(&self) -> String {
        quote(self.as_ref())
    }
}

==============================================================
*/

impl<T: chrono::TimeZone> ToSql for chrono::Date<T>
where
    T::Offset: std::fmt::Display,
{
    fn to_sql(&self) -> String {
        self.format("%Y-%m-%d").to_string().to_sql()
    }
}

impl<T: chrono::TimeZone> ToSql for chrono::DateTime<T>
where
    T::Offset: std::fmt::Display,
{
    fn to_sql(&self) -> String {
        self.format("%Y-%m-%d").to_string().to_sql()
    }
}

impl<T: Numeric + std::fmt::Display> ToSql for T {
    fn to_sql(&self) -> String {
        format!("{}", &self)
    }
}

impl<T: ToSql> ToSql for Vec<T> {
    fn op_eq(&self) -> &str {
        if self.len() > 1 {
            return "IN";
        }

        return "=";
    }

    fn op_neq(&self) -> &str {
        if self.len() > 1 {
            return "NOT IN";
        }

        return "<>";
    }

    fn to_sql(&self) -> String {
        let v = self.iter().map(|x| x.to_sql()).collect::<Vec<String>>();
        if v.len() == 1 {
            return v[0].clone();
        }

        format!("({})", v.join(","))
    }
}

/*
=======================================================================
  Ideally we would like to abstract the case for Vec to any iterable,
  but Rust complains again. Something like the code below
-----------------------------------------------------------------------

impl<T> ToSql for T where
    T: IntoIterator,
    T::Item: ToSql + std::fmt::Display
{
    fn op_eq(&self) -> &str {
        return "IN";
    }

    fn op_neq(&self) -> &str {
        return "NOT IN";
    }

    fn to_sql(&self) -> String {
        let mut v = Vec::new();
        for val in &self.into_iter() {
            v.push(val.to_string());
        }
        format!("({})", v.join(","))
    }
}

*/

impl<T: ToSql> ToSql for Option<T> {
    fn op_eq(&self) -> &str {
        if self.is_none() {
            return "IS";
        }

        return "=";
    }

    fn op_neq(&self) -> &str {
        if self.is_none() {
            return "IS NOT";
        }

        return "<>";
    }

    fn to_sql(&self) -> String {
        self.as_ref().map_or("NULL".to_owned(), |v| v.to_sql())
    }
}

pub struct SQLFilter<T: ToSql> {
    pub column: String,
    pub filter: T,
    pub cmp: SQLComp,
}

pub trait Filter {
    fn apply_filter(&self) -> String;
}

impl<T: ToSql> Filter for SQLFilter<T> {
    fn apply_filter(&self) -> String {
        format!("{} {}", &self.column, &self.filter.compare(&self.cmp))
    }
}

pub struct SQLable {
    table: String,
    cols: Option<Vec<String>>,
    filter: Option<Vec<Box<dyn Filter>>>,
}

impl SQLable {
    pub fn new(tbl: &str) -> Self {
        SQLable {
            table: tbl.to_owned(),
            cols: None,
            filter: None,
        }
    }

    pub fn get_cols(&self) -> &Option<Vec<String>> {
        &self.cols
    }

    pub fn get_snake_cols(&self) -> Option<Vec<String>> {
        self.cols
            .as_ref()
            .map(|v| v.iter().map(|s| snake_case(s)).collect::<Vec<String>>())
    }

    fn prepare_select(&self) -> String {
        if self.cols.is_none() {
            return "*".to_owned();
        }
        let cols = self.cols.as_ref().unwrap();
        if cols.is_empty() {
            return "*".to_owned();
        }

        let mut result = String::new();

        for (idx, col) in cols.iter().enumerate() {
            result.push_str(&snake_case(col));
            if idx < (cols.len() - 1) {
                result.push(',');
            }
        }

        result
    }

    fn prepare_filter(&self) -> Vec<String> {
        if self.filter.is_none() {
            return vec![];
        }
        let filter = self.filter.as_ref().unwrap();
        let mut result: Vec<String> = Vec::with_capacity(filter.len());
        for val in filter {
            result.push(val.apply_filter());
        }
        result
    }

    pub fn select(&mut self, cols: Vec<String>) -> &mut Self {
        self.cols = Some(cols);
        self
    }

    pub fn filter(&mut self, cols: Vec<Box<dyn Filter>>) -> &mut Self {
        self.filter = Some(cols);
        self
    }

    pub fn prepare(&self) -> String {
        // - first build the SELECT statement
        let mut select = format!("SELECT\n  {}\n", self.prepare_select());
        // - then we build the FROM statement

        let mut from = format!("FROM {}\n", self.table);
        // - then we build the WHERE statement

        let f = self.prepare_filter();
        let mut whr = String::new();
        if !f.is_empty() {
            whr.push_str("WHERE\n");
            for (idx, val) in f.iter().enumerate() {
                whr.push_str("  ");

                if idx > 0 {
                    whr.push_str("AND ");
                }

                whr.push('(');
                whr.push_str(val);
                whr.push(')');
                whr.push('\n');
            }
        }

        let mut output = String::new();
        for ch in select.drain(..).chain(from.drain(..)).chain(whr.drain(..)) {
            output.push(ch);
        }

        output
    }
}

fn main() {

    let f1 = SQLFilter {
        column: "a".to_string(),
        filter: 1,
        cmp: SQLComp::EQ,
    };
    let f2 = SQLFilter {
        column: "b".to_string(),
        filter: 2.5,
        cmp: SQLComp::LT,
    };
    let f3 = SQLFilter {
        column: "c".to_string(),
        filter: chrono::Utc::today(),
        cmp: SQLComp::GEQ,
    };
    let f4: SQLFilter<Option<&str>> = SQLFilter {
        column: "d".to_string(),
        filter: None,
        cmp: SQLComp::NEQ,
    };
    let f5 = SQLFilter {
        column: "e".to_string(),
        filter: vec![1, 2, 3],
        cmp: SQLComp::EQ,
    };
    let f6 = SQLFilter {
        column: "f".to_string(),
        filter: vec!["a", "b", "c", "d"],
        cmp: SQLComp::NEQ,
    };

    let mut f: Vec<Box<dyn Filter>> = Vec::new();
    f.push(Box::new(f1));
    f.push(Box::new(f2));
    f.push(Box::new(f3));
    f.push(Box::new(f4));
    f.push(Box::new(f5));
    f.push(Box::new(f6));

    let tbl = SQLable {
        table: "tbl".to_string(),
        cols: None,
        filter: Some(f),
    };

    println!("{}", tbl.prepare());
}
