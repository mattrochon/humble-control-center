import { NavLink, Outlet } from "react-router-dom";
import SessionBadge from "./SessionBadge";

const linkClass = ({ isActive }: { isActive: boolean }) =>
  `nav-link${isActive ? " active" : ""}`;

export default function Layout() {
  return (
    <div className="app-shell">
      <header className="nav">
        <div className="brand">Humble Library</div>
        <nav className="links">
          <NavLink to="/" className={linkClass}>
            Home
          </NavLink>
          <NavLink to="/library" className={linkClass}>
            Library
          </NavLink>
          <NavLink to="/purchases" className={linkClass}>
            Purchases
          </NavLink>
          <NavLink to="/admin" className={linkClass}>
            Control Room
          </NavLink>
          <NavLink to="/settings" className={linkClass}>
            Settings
          </NavLink>
        </nav>
        <SessionBadge />
      </header>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
