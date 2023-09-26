use crate::metadata::{MdId, Metadata};

pub trait MdProvider {
    fn retrieve_metadata(&self, md_id: &Box<dyn MdId>) -> Option<Box<dyn Metadata>>;
}
