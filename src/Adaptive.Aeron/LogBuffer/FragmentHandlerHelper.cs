using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;

namespace Adaptive.Aeron.LogBuffer
{
    public static class FragmentHandlerHelper
    {
        private class FragmentHandlerWrapper : IFragmentHandler
        {
            private readonly FragmentHandler _delegate;

            public FragmentHandlerWrapper(FragmentHandler @delegate)
            {
                _delegate = @delegate;
            }

            public void OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                _delegate(buffer, offset, length, header);
            }
        }
        
        private class ControlledFragmentHandlerWrapper : IControlledFragmentHandler
        {
            private readonly ControlledFragmentHandler _delegate;

            public ControlledFragmentHandlerWrapper(ControlledFragmentHandler @delegate)
            {
                _delegate = @delegate;
            }

            public ControlledFragmentHandlerAction OnFragment(IDirectBuffer buffer, int offset, int length, Header header)
            {
                return _delegate(buffer, offset, length, header);
            }
        }

        public static IFragmentHandler ToFragmentHandler(FragmentHandler @delegate)
        {
            return new FragmentHandlerWrapper(@delegate);
        }
        
        public static IControlledFragmentHandler ToControlledFragmentHandler(ControlledFragmentHandler @delegate)
        {
            return new ControlledFragmentHandlerWrapper(@delegate);
        }
    }
}