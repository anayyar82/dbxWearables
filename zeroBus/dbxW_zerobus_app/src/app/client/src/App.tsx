import { createBrowserRouter, RouterProvider, Outlet } from 'react-router';
import { Navbar } from '@/components/Navbar';
import { HomePage } from '@/pages/home/HomePage';
import { HealthPage } from '@/pages/health/HealthPage';
import { DocsPage } from '@/pages/docs/DocsPage';
import { LakebasePage } from '@/pages/lakebase/LakebasePage';

function Layout() {
  return (
    <div className="min-h-screen bg-[var(--background)] flex flex-col">
      <Navbar />
      <main className="flex-1">
        <Outlet />
      </main>
      <Footer />
    </div>
  );
}

function Footer() {
  return (
    <footer className="bg-[var(--dbx-navy)] text-gray-400 text-xs py-6 px-6">
      <div className="max-w-7xl mx-auto flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="font-bold text-white">dbxWearables</span>
          <span className="text-gray-500">|</span>
          <span>ZeroBus Health Data Gateway</span>
        </div>
        <div className="flex items-center gap-4">
          <span>Powered by Databricks AppKit</span>
          <a
            href="https://docs.databricks.com/aws/en/ingestion/zerobus-overview/"
            target="_blank"
            rel="noopener noreferrer"
            className="text-[var(--dbx-red)] hover:text-[var(--dbx-orange)] transition-colors"
          >
            ZeroBus Docs →
          </a>
        </div>
      </div>
    </footer>
  );
}

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <HomePage /> },
      { path: '/health', element: <HealthPage /> },
      { path: '/docs', element: <DocsPage /> },
      { path: '/lakebase', element: <LakebasePage /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
