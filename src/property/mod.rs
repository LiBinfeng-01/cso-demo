pub mod sort_property;

use crate::memo::{GroupPlan, GroupRef};
use crate::property::sort_property::SortProperty;

pub trait Property {}
pub trait LogicalProperty: Property {}
pub trait PhysicalProperty: Property {
    fn make_enforcer(&self, inputs: Vec<GroupRef>) -> GroupPlan;
}

#[derive(Clone)]
pub struct LogicalProperties {}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct PhysicalProperties {
    order_spec: SortProperty,
}

impl PhysicalProperties {
    pub fn new() -> PhysicalProperties {
        PhysicalProperties {
            order_spec: SortProperty::new(),
        }
    }

    pub fn satisfy(&self, _required_prop: &PhysicalProperties) -> bool {
        // all output properties should be super set of required one
        self.order_spec.satisfy(&_required_prop.order_spec)
    }

    pub fn make_enforcer(&self, inputs: Vec<GroupRef>) -> GroupPlan {
        self.order_spec.make_enforcer(inputs)
    }
}
