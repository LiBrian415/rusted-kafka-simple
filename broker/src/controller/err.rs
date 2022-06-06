use std::{error::Error, fmt::Display};

#[derive(Debug, Clone)]
pub enum ControllerError {
    // used when the broker doesn't exist
    MissingBroker(u32),
    // used when the controller is not set
    MissingController(),
    // used when a non-controller performs a controller task
    NotTheController(u32),
    // used when number of brokers is less than required replicas
    NotEnoughBrokers(),
    // used when the topic doesn't exist
    MissingTopic(String),
}

impl Display for ControllerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let x = match self {
            ControllerError::MissingBroker(id) => {
                format!("broker \"{}\" does not exist", id)
            }
            ControllerError::MissingController() => {
                format!("controller does not exist")
            }
            ControllerError::NotTheController(id) => {
                format!("broker \"{}\" is not the controller", id)
            }
            ControllerError::NotEnoughBrokers() => {
                format!("number of available brokers is less than replicas")
            }
            ControllerError::MissingTopic(topic) => {
                format!("topic {} doesn't exist", topic)
            }
        };
        write!(f, "{}", x)
    }
}

impl Error for ControllerError {}

pub type ControllerResult<T> = Result<T, Box<(dyn Error + Send + Sync)>>;
