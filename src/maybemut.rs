use std::borrow::Borrow;

pub struct RO;

pub struct RW;

// FIXME: Replace this with Borrow<T>

pub trait MaybeMut<'a, T> {
    type Reference: Borrow<T>;
}

impl<'a, T: 'a> MaybeMut<'a, T> for RO {
    type Reference = &'a T;
}

impl<'a, T: 'a> MaybeMut<'a, T> for RW {
    type Reference = &'a mut T;
}
