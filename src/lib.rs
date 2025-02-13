//! A cascade style optimizer

#![forbid(unsafe_code)]

use crate::memo::Memo;
use crate::task::{OptimizeGroupTask, Task, TaskRunner};

mod memo;
mod task;

pub trait LogicalOperator {}
pub trait PhysicalOperator {}

pub enum Operator {
    Logical(Box<dyn LogicalOperator>),
    Physical(Box<dyn PhysicalOperator>),
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
}

pub trait ScalarExpression {}
pub trait AggregateExpression {}

pub struct LogicalPlan {
    op: Box<dyn LogicalOperator>,
    inputs: Vec<LogicalPlan>,
    _required_properties: Vec<PhysicalProperties>,
}

pub struct PhysicalPlan {
    _op: Box<dyn PhysicalOperator>,
    _inputs: Vec<PhysicalPlan>,
}

pub trait Property {}
pub trait LogicalProperty: Property {}
pub trait PhysicalProperty: Property {}

pub struct LogicalProperties {}
pub struct PhysicalProperties {}

pub struct Options {}

pub struct Optimizer {
    _options: Options,
}

impl Optimizer {
    pub fn new(_options: Options) -> Optimizer {
        Optimizer { _options }
    }

    pub fn optimize(
        &mut self,
        plan: LogicalPlan,
        _required_properties: PhysicalProperties,
    ) -> PhysicalPlan {
        let mut optimizer_ctx = OptimizerContext::new();
        optimizer_ctx.memo_mut().init(plan);
        let mut task_runner = TaskRunner::new();
        let initial_task = OptimizeGroupTask::new(optimizer_ctx.memo().root_group_id());
        task_runner.push_task(Task::OptimizeGroup(initial_task));
        task_runner.run(&mut optimizer_ctx);
        todo!()
    }
}

pub struct OptimizerContext {
    memo: Memo,
}

impl OptimizerContext {
    fn new() -> Self {
        OptimizerContext { memo: Memo::new() }
    }

    pub fn memo_mut(&mut self) -> &mut Memo {
        &mut self.memo
    }

    pub fn memo(&self) -> &Memo {
        &self.memo
    }
}
