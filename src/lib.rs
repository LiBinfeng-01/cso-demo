//! A cascade style optimizer

#![forbid(unsafe_code)]

pub mod rule;

mod memo;
mod statistics;
mod task;

use std::hash::{Hash, Hasher};
use crate::memo::{GroupPlanRef, Memo};
use crate::rule::RuleSet;
use crate::statistics::Statistics;
use crate::task::{OptimizeGroupTask, Task, TaskRunner};
use std::rc::Rc;

pub trait LogicalOperator {
    fn name(&self) -> &str;
    fn operator_id(&self) -> i16;
    fn derive_statistics(&self, _input_stats: &[Rc<Statistics>]) -> Statistics;
}

pub trait PhysicalOperator {
    fn name(&self) -> &str;
    fn operator_id(&self) -> i16;
    fn derive_output_prop(&self, _: &Vec<&PhysicalProperties>) -> PhysicalProperties;
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
    pub fn derive_statistics(&self, input_stats: &[Rc<Statistics>]) -> Statistics {
        match self {
            Operator::Logical(op) => op.derive_statistics(input_stats),
            Operator::Physical(_) => unreachable!("only logical operators can derive statistics"),
        }
    }

    #[inline]
    pub fn derive_output_prop(&self, input_props: &Vec<&PhysicalProperties>) -> PhysicalProperties {
        match self {
            Operator::Logical(_) => unreachable!("only physical operators can derive output props"),
            Operator::Physical(op) => op.derive_output_prop(input_props),
        }
    }
}

pub struct SortOperator {
    order_keys: Vec<Box <dyn ScalarExpression>>,
}

impl PhysicalOperator for SortOperator {
    fn name(&self) -> &str {
        "SortOperator"
    }

    fn operator_id(&self) -> i16 {
        todo!()
    }

    fn derive_output_prop(&self, _: &Vec<&PhysicalProperties>) -> PhysicalProperties {
        todo!()
    }
}

impl SortOperator {
    pub fn new(input_order_keys: Vec<Box<dyn ScalarExpression>>) -> SortOperator {
        SortOperator {
            order_keys: input_order_keys,
        }
    }
}

pub trait ScalarExpression : Clone + Hash + PartialEq + Eq {}
pub trait AggregateExpression {}

pub struct LogicalPlan {
    op: Rc<dyn LogicalOperator>,
    inputs: Vec<LogicalPlan>,
    _required_properties: Vec<PhysicalProperties>,
}

pub struct PhysicalPlan {
    _op: Rc<dyn PhysicalOperator>,
    _inputs: Vec<PhysicalPlan>,
}

pub struct Plan {
    op: Operator,
    inputs: Vec<Plan>,
    _property: LogicalProperties,
    group_plan: Option<GroupPlanRef>,
    _required_properties: Vec<PhysicalProperties>,
}

impl Plan {
    pub fn new(op: Operator, inputs: Vec<Plan>) -> Self {
        Plan {
            op,
            inputs,
            _property: LogicalProperties {},
            group_plan: None,
            _required_properties: vec![],
        }
    }

    pub fn inputs(&self) -> &[Plan] {
        &self.inputs
    }

    pub fn group_plan(&self) -> Option<&GroupPlanRef> {
        self.group_plan.as_ref()
    }
}

pub trait Property {}
pub trait LogicalProperty: Property {}
pub trait PhysicalProperty: Property {
    fn satisfy(&self, _input: PhysicalProperties) -> bool where Self: Sized {
        true
    }
    fn add_enforcer(&self, physical_op: Operator, inputs: Vec<Plan>) -> Plan where Self: Sized {
        Plan::new(physical_op, inputs)
    }
}

#[derive(PartialEq, PartialOrd)]
pub struct Cost {
    _cost: f64,
}

impl Cost {
    pub const fn new() -> Cost {
        Cost { _cost : 0.0 }
    }
}
pub struct LogicalProperties {}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct PhysicalProperties {
    _order_spec: SortProperty,
}

impl PhysicalProperties {
    pub fn new() -> PhysicalProperties {
        PhysicalProperties {
            _order_spec: SortProperty::new(),
        }
    }

    pub fn satisfy(&self, required_prop: &PhysicalProperties) -> bool {
        todo!()
    }
}

#[derive(Clone, Hash, Eq)]
pub struct SortProperty {
    _order_keys: Vec<Box<dyn ScalarExpression>>,
}

impl PartialEq for SortProperty {
    fn eq(&self, other: &Self) -> bool {
        todo!()
    }
}

impl Property for SortProperty {}

impl PhysicalProperty for SortProperty {
    fn satisfy(&self, intput: PhysicalProperties) -> bool {
        todo!()
    }

    fn add_enforcer(&self, physical_op: Operator, inputs: Vec<Plan>) -> Plan {
        todo!()
    }
}

impl SortProperty {
    pub fn new() -> SortProperty {
        SortProperty { _order_keys: vec![] }
    }

    pub fn make_plan(self, inputs: Vec<Plan>) -> Plan {
        todo!()
    }
}

pub struct Options {}

pub struct Optimizer {
    _options: Options,
}

impl Optimizer {
    pub fn new(_options: Options) -> Optimizer {
        Optimizer { _options }
    }

    pub fn optimize(&mut self, plan: LogicalPlan, required_properties: PhysicalProperties) -> PhysicalPlan {
        let mut optimizer_ctx = OptimizerContext::new();
        optimizer_ctx.memo_mut().init(plan);
        let mut task_runner = TaskRunner::new();
        let initial_task = OptimizeGroupTask::new(optimizer_ctx.memo().root_group().clone(), required_properties);
        task_runner.push_task(Task::OptimizeGroup(initial_task));
        task_runner.run(&mut optimizer_ctx);
        todo!()
    }
}

pub struct OptimizerContext {
    memo: Memo,
    rule_set: RuleSet,
}

impl OptimizerContext {
    fn new() -> Self {
        OptimizerContext {
            memo: Memo::new(),
            rule_set: RuleSet::new(),
        }
    }

    pub fn memo_mut(&mut self) -> &mut Memo {
        &mut self.memo
    }

    pub fn memo(&self) -> &Memo {
        &self.memo
    }

    pub fn rule_set(&self) -> &RuleSet {
        &self.rule_set
    }
}
