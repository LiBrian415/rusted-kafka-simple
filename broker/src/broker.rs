#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Void {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProducerInput {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partition: u32,
    #[prost(bytes = "vec", tag = "3")]
    pub message_set: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "4")]
    pub required_acks: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumerInput {
    #[prost(int32, tag = "1")]
    pub replica_id: i32,
    #[prost(string, tag = "2")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub partition: u32,
    #[prost(uint64, tag = "4")]
    pub offset: u64,
    #[prost(uint64, tag = "5")]
    pub max_bytes: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsumerOutput {
    #[prost(bytes = "vec", tag = "1")]
    pub messages: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub watermark: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicInput {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partitions: u32,
    #[prost(uint32, tag = "3")]
    pub replicas: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BrokerInfo {
    #[prost(string, tag = "1")]
    pub hostname: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub port: ::prost::alloc::string::String,
    #[prost(uint32, tag = "3")]
    pub replica_id: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicPartition {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub partition: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaderOrFollow {
    #[prost(map = "string, string", tag = "1")]
    pub leader:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(map = "string, string", tag = "2")]
    pub follow:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Topic {
    #[prost(string, tag = "1")]
    pub topic: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Partitions {
    #[prost(uint32, repeated, tag = "1")]
    pub partitions: ::prost::alloc::vec::Vec<u32>,
}
#[doc = r" Generated client implementations."]
pub mod broker_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[derive(Debug, Clone)]
    pub struct BrokerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl BrokerClient<tonic::transport::Channel> {
        #[doc = r" Attempt to create a new client by connecting to a given endpoint."]
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> BrokerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::ResponseBody: Body + Send + 'static,
        T::Error: Into<StdError>,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> BrokerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<http::Request<tonic::body::BoxBody>>>::Error:
                Into<StdError> + Send + Sync,
        {
            BrokerClient::new(InterceptedService::new(inner, interceptor))
        }
        #[doc = r" Compress requests with `gzip`."]
        #[doc = r""]
        #[doc = r" This requires the server to support it otherwise it might respond with an"]
        #[doc = r" error."]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        #[doc = r" Enable decompressing responses with `gzip`."]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        pub async fn get_controller(
            &mut self,
            request: impl tonic::IntoRequest<super::Void>,
        ) -> Result<tonic::Response<super::BrokerInfo>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/get_controller");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_leader(
            &mut self,
            request: impl tonic::IntoRequest<super::TopicPartition>,
        ) -> Result<tonic::Response<super::BrokerInfo>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/get_leader");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_topic(
            &mut self,
            request: impl tonic::IntoRequest<super::TopicInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/create_topic");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn produce(
            &mut self,
            request: impl tonic::IntoRequest<super::ProducerInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/produce");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn consume(
            &mut self,
            request: impl tonic::IntoRequest<super::ConsumerInput>,
        ) -> Result<tonic::Response<super::ConsumerOutput>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/consume");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn set_leader_or_follow(
            &mut self,
            request: impl tonic::IntoRequest<super::LeaderOrFollow>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/set_leader_or_follow");
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn get_partitions(
            &mut self,
            request: impl tonic::IntoRequest<super::Topic>,
        ) -> Result<tonic::Response<super::Partitions>, tonic::Status> {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::new(
                    tonic::Code::Unknown,
                    format!("Service was not ready: {}", e.into()),
                )
            })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/broker.Broker/get_partitions");
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
#[doc = r" Generated server implementations."]
pub mod broker_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    #[doc = "Generated trait containing gRPC methods that should be implemented for use with BrokerServer."]
    #[async_trait]
    pub trait Broker: Send + Sync + 'static {
        async fn get_controller(
            &self,
            request: tonic::Request<super::Void>,
        ) -> Result<tonic::Response<super::BrokerInfo>, tonic::Status>;
        async fn get_leader(
            &self,
            request: tonic::Request<super::TopicPartition>,
        ) -> Result<tonic::Response<super::BrokerInfo>, tonic::Status>;
        async fn create_topic(
            &self,
            request: tonic::Request<super::TopicInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status>;
        async fn produce(
            &self,
            request: tonic::Request<super::ProducerInput>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status>;
        async fn consume(
            &self,
            request: tonic::Request<super::ConsumerInput>,
        ) -> Result<tonic::Response<super::ConsumerOutput>, tonic::Status>;
        async fn set_leader_or_follow(
            &self,
            request: tonic::Request<super::LeaderOrFollow>,
        ) -> Result<tonic::Response<super::Void>, tonic::Status>;
        async fn get_partitions(
            &self,
            request: tonic::Request<super::Topic>,
        ) -> Result<tonic::Response<super::Partitions>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct BrokerServer<T: Broker> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Broker> BrokerServer<T> {
        pub fn new(inner: T) -> Self {
            let inner = Arc::new(inner);
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(inner: T, interceptor: F) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for BrokerServer<T>
    where
        T: Broker,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = Never;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/broker.Broker/get_controller" => {
                    #[allow(non_camel_case_types)]
                    struct get_controllerSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::Void> for get_controllerSvc<T> {
                        type Response = super::BrokerInfo;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Void>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_controller(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = get_controllerSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/get_leader" => {
                    #[allow(non_camel_case_types)]
                    struct get_leaderSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::TopicPartition> for get_leaderSvc<T> {
                        type Response = super::BrokerInfo;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TopicPartition>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_leader(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = get_leaderSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/create_topic" => {
                    #[allow(non_camel_case_types)]
                    struct create_topicSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::TopicInput> for create_topicSvc<T> {
                        type Response = super::Void;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::TopicInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).create_topic(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = create_topicSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/produce" => {
                    #[allow(non_camel_case_types)]
                    struct produceSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::ProducerInput> for produceSvc<T> {
                        type Response = super::Void;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ProducerInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).produce(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = produceSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/consume" => {
                    #[allow(non_camel_case_types)]
                    struct consumeSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::ConsumerInput> for consumeSvc<T> {
                        type Response = super::ConsumerOutput;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ConsumerInput>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).consume(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = consumeSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/set_leader_or_follow" => {
                    #[allow(non_camel_case_types)]
                    struct set_leader_or_followSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::LeaderOrFollow> for set_leader_or_followSvc<T> {
                        type Response = super::Void;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::LeaderOrFollow>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).set_leader_or_follow(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = set_leader_or_followSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/broker.Broker/get_partitions" => {
                    #[allow(non_camel_case_types)]
                    struct get_partitionsSvc<T: Broker>(pub Arc<T>);
                    impl<T: Broker> tonic::server::UnaryService<super::Topic> for get_partitionsSvc<T> {
                        type Response = super::Partitions;
                        type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                        fn call(&mut self, request: tonic::Request<super::Topic>) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move { (*inner).get_partitions(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = get_partitionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec).apply_compression_config(
                            accept_compression_encodings,
                            send_compression_encodings,
                        );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => Box::pin(async move {
                    Ok(http::Response::builder()
                        .status(200)
                        .header("grpc-status", "12")
                        .header("content-type", "application/grpc")
                        .body(empty_body())
                        .unwrap())
                }),
            }
        }
    }
    impl<T: Broker> Clone for BrokerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Broker> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Broker> tonic::transport::NamedService for BrokerServer<T> {
        const NAME: &'static str = "broker.Broker";
    }
}
