package subserver.dataplane.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.68.2)",
    comments = "Source: data_plane.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class DataPlaneGrpc {

  private DataPlaneGrpc() {}

  public static final java.lang.String SERVICE_NAME = "subserver.dataplane.v1.DataPlane";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest,
      subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope> getSubscribeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Subscribe",
      requestType = subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest.class,
      responseType = subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest,
      subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope> getSubscribeMethod() {
    io.grpc.MethodDescriptor<subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest, subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope> getSubscribeMethod;
    if ((getSubscribeMethod = DataPlaneGrpc.getSubscribeMethod) == null) {
      synchronized (DataPlaneGrpc.class) {
        if ((getSubscribeMethod = DataPlaneGrpc.getSubscribeMethod) == null) {
          DataPlaneGrpc.getSubscribeMethod = getSubscribeMethod =
              io.grpc.MethodDescriptor.<subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest, subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Subscribe"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope.getDefaultInstance()))
              .setSchemaDescriptor(new DataPlaneMethodDescriptorSupplier("Subscribe"))
              .build();
        }
      }
    }
    return getSubscribeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DataPlaneStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataPlaneStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataPlaneStub>() {
        @java.lang.Override
        public DataPlaneStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataPlaneStub(channel, callOptions);
        }
      };
    return DataPlaneStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DataPlaneBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataPlaneBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataPlaneBlockingStub>() {
        @java.lang.Override
        public DataPlaneBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataPlaneBlockingStub(channel, callOptions);
        }
      };
    return DataPlaneBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DataPlaneFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DataPlaneFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DataPlaneFutureStub>() {
        @java.lang.Override
        public DataPlaneFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DataPlaneFutureStub(channel, callOptions);
        }
      };
    return DataPlaneFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void subscribe(subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest request,
        io.grpc.stub.StreamObserver<subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubscribeMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DataPlane.
   */
  public static abstract class DataPlaneImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DataPlaneGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DataPlane.
   */
  public static final class DataPlaneStub
      extends io.grpc.stub.AbstractAsyncStub<DataPlaneStub> {
    private DataPlaneStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataPlaneStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataPlaneStub(channel, callOptions);
    }

    /**
     */
    public void subscribe(subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest request,
        io.grpc.stub.StreamObserver<subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getSubscribeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DataPlane.
   */
  public static final class DataPlaneBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DataPlaneBlockingStub> {
    private DataPlaneBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataPlaneBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataPlaneBlockingStub(channel, callOptions);
    }

    /**
     */
    public java.util.Iterator<subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope> subscribe(
        subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getSubscribeMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DataPlane.
   */
  public static final class DataPlaneFutureStub
      extends io.grpc.stub.AbstractFutureStub<DataPlaneFutureStub> {
    private DataPlaneFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DataPlaneFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DataPlaneFutureStub(channel, callOptions);
    }
  }

  private static final int METHODID_SUBSCRIBE = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SUBSCRIBE:
          serviceImpl.subscribe((subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest) request,
              (io.grpc.stub.StreamObserver<subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getSubscribeMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              subserver.dataplane.v1.DataPlaneOuterClass.SubscribeRequest,
              subserver.dataplane.v1.DataPlaneOuterClass.DataEnvelope>(
                service, METHODID_SUBSCRIBE)))
        .build();
  }

  private static abstract class DataPlaneBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DataPlaneBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return subserver.dataplane.v1.DataPlaneOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DataPlane");
    }
  }

  private static final class DataPlaneFileDescriptorSupplier
      extends DataPlaneBaseDescriptorSupplier {
    DataPlaneFileDescriptorSupplier() {}
  }

  private static final class DataPlaneMethodDescriptorSupplier
      extends DataPlaneBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DataPlaneMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (DataPlaneGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DataPlaneFileDescriptorSupplier())
              .addMethod(getSubscribeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
