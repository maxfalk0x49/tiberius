use crate::{IntoSql, TokenRow};

/// create a TokenRow from list of values
pub trait IntoRow<'a> {
    /// create a TokenRow from list of values which implements IntoSQL
    fn into_row(self) -> TokenRow<'a>;
}

impl<'a, A> IntoRow<'a> for A
where
    A: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(1);
        row.push(self.into_sql());
        row
    }
}

impl<'a, A, B> IntoRow<'a> for (A, B)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(2);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row
    }
}

impl<'a, A, B, C> IntoRow<'a> for (A, B, C)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(3);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row
    }
}

impl<'a, A, B, C, D> IntoRow<'a> for (A, B, C, D)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(4);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E> IntoRow<'a> for (A, B, C, D, E)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(5);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F> IntoRow<'a> for (A, B, C, D, E, F)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(6);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G> IntoRow<'a> for (A, B, C, D, E, F, G)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(7);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G, H> IntoRow<'a> for (A, B, C, D, E, F, G, H)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(8);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G, H, I> IntoRow<'a> for (A, B, C, D, E, F, G, H, I)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(9);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row
    }
}

impl<'a, A, B, C, D, E, F, G, H, I, J> IntoRow<'a> for (A, B, C, D, E, F, G, H, I, J)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
    J: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(10);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row
    }
}

// Implementation for 11-tuple
impl<'a, A, B, C, D, E, F, G, H, I, J, K> IntoRow<'a> for (A, B, C, D, E, F, G, H, I, J, K)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
    J: IntoSql<'a>,
    K: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(11);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row.push(self.10.into_sql());
        row
    }
}

// Implementation for 12-tuple
impl<'a, A, B, C, D, E, F, G, H, I, J, K, L> IntoRow<'a> for (A, B, C, D, E, F, G, H, I, J, K, L)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
    J: IntoSql<'a>,
    K: IntoSql<'a>,
    L: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(12);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row.push(self.10.into_sql());
        row.push(self.11.into_sql());
        row
    }
}

// Implementation for 13-tuple
impl<'a, A, B, C, D, E, F, G, H, I, J, K, L, M> IntoRow<'a>
    for (A, B, C, D, E, F, G, H, I, J, K, L, M)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
    J: IntoSql<'a>,
    K: IntoSql<'a>,
    L: IntoSql<'a>,
    M: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(13);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row.push(self.10.into_sql());
        row.push(self.11.into_sql());
        row.push(self.12.into_sql());
        row
    }
}

// Implementation for 14-tuple
impl<'a, A, B, C, D, E, F, G, H, I, J, K, L, M, N> IntoRow<'a>
    for (A, B, C, D, E, F, G, H, I, J, K, L, M, N)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
    J: IntoSql<'a>,
    K: IntoSql<'a>,
    L: IntoSql<'a>,
    M: IntoSql<'a>,
    N: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(14);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row.push(self.10.into_sql());
        row.push(self.11.into_sql());
        row.push(self.12.into_sql());
        row.push(self.13.into_sql());
        row
    }
}

// Implementation for 15-tuple
impl<'a, A, B, C, D, E, F, G, H, I, J, K, L, M, N, O> IntoRow<'a>
    for (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)
where
    A: IntoSql<'a>,
    B: IntoSql<'a>,
    C: IntoSql<'a>,
    D: IntoSql<'a>,
    E: IntoSql<'a>,
    F: IntoSql<'a>,
    G: IntoSql<'a>,
    H: IntoSql<'a>,
    I: IntoSql<'a>,
    J: IntoSql<'a>,
    K: IntoSql<'a>,
    L: IntoSql<'a>,
    M: IntoSql<'a>,
    N: IntoSql<'a>,
    O: IntoSql<'a>,
{
    fn into_row(self) -> TokenRow<'a> {
        let mut row = TokenRow::with_capacity(15);
        row.push(self.0.into_sql());
        row.push(self.1.into_sql());
        row.push(self.2.into_sql());
        row.push(self.3.into_sql());
        row.push(self.4.into_sql());
        row.push(self.5.into_sql());
        row.push(self.6.into_sql());
        row.push(self.7.into_sql());
        row.push(self.8.into_sql());
        row.push(self.9.into_sql());
        row.push(self.10.into_sql());
        row.push(self.11.into_sql());
        row.push(self.12.into_sql());
        row.push(self.13.into_sql());
        row.push(self.14.into_sql());
        row
    }
}
