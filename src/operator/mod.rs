pub mod logical_filter;
pub mod logical_project;
pub mod logical_scan;
pub mod physical_filter;
pub mod physical_project;
pub mod physical_scan;
pub mod physical_topn;

use crate::metadata::MdAccessor;
use crate::property::PhysicalProperties;
use crate::statistics::Statistics;
use std::any::Any;
use std::rc::Rc;
use std::todo;

pub trait LogicalOperator: Any {
    fn name(&self) -> &str;
    fn as_any(&self) -> &dyn Any;
    fn operator_id(&self) -> i16;
    fn derive_statistics(&self, md_accessor: &MdAccessor, input_stats: &[Rc<Statistics>]) -> Statistics;
}

pub trait PhysicalOperator: Any {
    fn name(&self) -> &str;
    fn operator_id(&self) -> i16;
    fn derive_output_prop(&self, _: &[Rc<PhysicalProperties>]) -> PhysicalProperties;
}

#[derive(Clone)]
pub enum Operator {
    Logical(Rc<dyn LogicalOperator>),
    Physical(Rc<dyn PhysicalOperator>),
}

impl Operator {
    #[inline]
    pub fn is_logical(&self) -> bool {
        match self {
            Operator::Logical(_) => true,
            Operator::Physical(_) => false,
        }
    }

    #[inline]
    pub fn is_physical(&self) -> bool {
        match self {
            Operator::Logical(_) => false,
            Operator::Physical(_) => true,
        }
    }

    #[inline]
    pub fn logical_op(&self) -> &Rc<dyn LogicalOperator> {
        match self {
            Operator::Logical(op) => op,
            Operator::Physical(_) => unreachable!("expect logical operator"),
        }
    }

    #[inline]
    pub fn physical_op(&self) -> &Rc<dyn PhysicalOperator> {
        match self {
            Operator::Logical(_) => unreachable!("expect physical operator"),
            Operator::Physical(op) => op,
        }
    }

    #[inline]
    pub fn derive_output_prop(&self, input_props: &[Rc<PhysicalProperties>]) -> PhysicalProperties {
        match self {
            Operator::Logical(_) => unreachable!("only physical operators can derive output props"),
            Operator::Physical(op) => op.derive_output_prop(input_props),
        }
    }

    #[inline]
    pub fn get_reqd_prop(&self) -> Vec<Vec<PhysicalProperties>> {
        todo!()
    }
}
